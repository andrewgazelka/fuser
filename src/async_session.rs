//! Multi-threaded async FUSE session.
//!
//! This module provides a session implementation that uses multiple reader threads
//! to achieve high IOPS, with each request dispatched as an async tokio task.

use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::JoinHandle;

use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{debug, error, info, warn};
use nix::unistd::geteuid;

#[cfg(any(
    fuser_mount_impl = "pure-rust",
    fuser_mount_impl = "libfuse2",
    fuser_mount_impl = "libfuse3"
))]
use crate::MountOption;
use crate::async_fs::AsyncFilesystem;
use crate::channel::{Channel, ChannelSender};
use crate::ll::fuse_abi as abi;
#[cfg(any(
    fuser_mount_impl = "pure-rust",
    fuser_mount_impl = "libfuse2",
    fuser_mount_impl = "libfuse3"
))]
use crate::mnt::Mount;
use crate::notify::Notifier;
use crate::owned_request::{FuseReader, OwnedRequest};
use crate::session::{MAX_WRITE_SIZE, SessionACL};

/// Size of the buffer for reading a request from the kernel.
const BUFFER_SIZE: usize = MAX_WRITE_SIZE + 4096;

/// ioctl to clone /dev/fuse fd for multi-threaded operation.
#[cfg(target_os = "linux")]
const FUSE_DEV_IOC_CLONE: libc::c_ulong = 0x8004_e500;

/// Clone a /dev/fuse file descriptor for multi-threaded reading.
///
/// On Linux, this uses the `FUSE_DEV_IOC_CLONE` ioctl to create a new fd
/// that shares the same FUSE connection but can be read independently.
#[cfg(target_os = "linux")]
fn clone_fuse_fd(fd: &impl AsRawFd) -> io::Result<OwnedFd> {
    // Open a new /dev/fuse fd
    let new_fd = unsafe { libc::open(c"/dev/fuse".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if new_fd < 0 {
        return Err(io::Error::last_os_error());
    }

    // Clone the session onto the new fd
    let original_fd = fd.as_raw_fd();
    let result = unsafe { libc::ioctl(new_fd, FUSE_DEV_IOC_CLONE, &original_fd as *const _) };

    if result < 0 {
        let err = io::Error::last_os_error();
        unsafe { libc::close(new_fd) };
        return Err(err);
    }

    // SAFETY: new_fd is a valid file descriptor from open() + successful ioctl
    Ok(unsafe { OwnedFd::from_raw_fd(new_fd) })
}

/// Stub for non-Linux platforms.
#[cfg(not(target_os = "linux"))]
fn clone_fuse_fd(_fd: &impl AsRawFd) -> io::Result<OwnedFd> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "FUSE fd cloning only supported on Linux",
    ))
}

/// Shared state for an async FUSE session.
///
/// This is shared between all reader threads and contains the session
/// configuration and state that doesn't change after initialization.
#[derive(Debug)]
pub struct SharedSessionState {
    /// Whether to restrict access to owner, root + owner, or unrestricted
    pub allowed: SessionACL,
    /// User that launched the fuser process
    pub session_owner: u32,
    /// FUSE protocol major version
    pub proto_major: AtomicU32,
    /// FUSE protocol minor version
    pub proto_minor: AtomicU32,
    /// True if the filesystem is initialized
    pub initialized: AtomicBool,
    /// True if the filesystem was destroyed
    pub destroyed: AtomicBool,
    /// Signal to stop all reader threads
    pub shutdown: AtomicBool,
}

impl SharedSessionState {
    /// Create new shared state with the given ACL.
    pub fn new(allowed: SessionACL) -> Self {
        Self {
            allowed,
            session_owner: geteuid().as_raw(),
            proto_major: AtomicU32::new(0),
            proto_minor: AtomicU32::new(0),
            initialized: AtomicBool::new(false),
            destroyed: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
        }
    }
}

/// Placeholder type for when no mount implementation is available.
#[cfg(fuser_mount_impl = "none")]
#[derive(Debug)]
pub struct NoMount;

/// Multi-threaded async FUSE session.
///
/// This session spawns multiple reader threads, each reading from a cloned
/// `/dev/fuse` fd. Requests are dispatched as tokio tasks for concurrent processing.
///
/// # Type Parameters
///
/// * `FS` - The async filesystem implementation (must be `Send + Sync + 'static`)
pub struct AsyncSession<FS: AsyncFilesystem> {
    /// Filesystem implementation (wrapped in Arc for sharing)
    filesystem: Arc<FS>,
    /// Primary communication channel to the kernel
    channel: Channel,
    /// Shared session state
    state: Arc<SharedSessionState>,
    /// Handle to the mount (dropping this unmounts)
    #[cfg(any(
        fuser_mount_impl = "pure-rust",
        fuser_mount_impl = "libfuse2",
        fuser_mount_impl = "libfuse3"
    ))]
    mount: Option<(PathBuf, Mount)>,
    #[cfg(fuser_mount_impl = "none")]
    mount: Option<(PathBuf, NoMount)>,
    /// Tokio runtime handle for spawning async tasks
    runtime: tokio::runtime::Handle,
}

impl<FS: AsyncFilesystem> std::fmt::Debug for AsyncSession<FS> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncSession")
            .field("channel", &self.channel)
            .field("state", &self.state)
            .field("mount", &self.mount)
            .finish_non_exhaustive()
    }
}

impl<FS: AsyncFilesystem> AsyncSession<FS> {
    /// Create a new async session by mounting the filesystem at the given path.
    ///
    /// # Arguments
    ///
    /// * `filesystem` - The async filesystem implementation
    /// * `mountpoint` - Path to mount at
    /// * `options` - Mount options
    /// * `runtime` - Tokio runtime handle for async task spawning
    ///
    /// # Errors
    ///
    /// Returns an error if the mount options are invalid or mounting fails.
    #[cfg(any(
        fuser_mount_impl = "pure-rust",
        fuser_mount_impl = "libfuse2",
        fuser_mount_impl = "libfuse3"
    ))]
    pub fn new<P: AsRef<Path>>(
        filesystem: FS,
        mountpoint: P,
        options: &[MountOption],
        runtime: tokio::runtime::Handle,
    ) -> io::Result<Self> {
        let mountpoint = mountpoint.as_ref();
        info!("Mounting async FUSE at {}", mountpoint.display());

        // Handle AutoUnmount + implicit AllowOther (same as sync Session)
        let (file, mount) = if options.contains(&MountOption::AutoUnmount)
            && !(options.contains(&MountOption::AllowRoot)
                || options.contains(&MountOption::AllowOther))
        {
            warn!(
                "Given auto_unmount without allow_root or allow_other; \
                adding allow_other, with userspace permission handling"
            );
            let mut modified_options = options.to_vec();
            modified_options.push(MountOption::AllowOther);
            Mount::new(mountpoint, &modified_options)?
        } else {
            Mount::new(mountpoint, options)?
        };

        let channel = Channel::new(file);
        let allowed = if options.contains(&MountOption::AllowRoot) {
            SessionACL::RootAndOwner
        } else if options.contains(&MountOption::AllowOther) {
            SessionACL::All
        } else {
            SessionACL::Owner
        };

        Ok(Self {
            filesystem: Arc::new(filesystem),
            channel,
            state: Arc::new(SharedSessionState::new(allowed)),
            mount: Some((mountpoint.to_owned(), mount)),
            runtime,
        })
    }

    /// Create an async session from an existing `/dev/fuse` fd.
    ///
    /// This doesn't mount the filesystem; mounting must be done separately.
    pub fn from_fd(
        filesystem: FS,
        fd: OwnedFd,
        acl: SessionACL,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let channel = Channel::new(Arc::new(fd.into()));
        Self {
            filesystem: Arc::new(filesystem),
            channel,
            state: Arc::new(SharedSessionState::new(acl)),
            mount: None,
            runtime,
        }
    }

    /// Run the session with multiple reader threads.
    ///
    /// Each reader thread reads requests from the kernel and spawns tokio tasks
    /// to handle them concurrently.
    ///
    /// # Arguments
    ///
    /// * `num_threads` - Number of reader threads (minimum 1)
    ///
    /// # Errors
    ///
    /// Returns an error if creating reader threads fails or the session encounters
    /// an unrecoverable error.
    pub fn run(self, num_threads: usize) -> io::Result<()> {
        let num_threads = num_threads.max(1);

        if num_threads == 1 {
            // Single-threaded mode - run directly on this thread
            let reader = FuseReader::new(self.channel.sender());
            return run_reader_loop(
                reader,
                Arc::clone(&self.filesystem),
                Arc::clone(&self.state),
                self.runtime.clone(),
            );
        }

        // Multi-threaded mode - spawn additional reader threads
        let mut handles: Vec<JoinHandle<io::Result<()>>> = Vec::with_capacity(num_threads);

        // Prepare thread 0's resources (uses original fd)
        let reader0 = FuseReader::new(self.channel.sender());
        let fs0 = Arc::clone(&self.filesystem);
        let state0 = Arc::clone(&self.state);
        let rt0 = self.runtime.clone();

        // Clone fds and spawn additional reader threads
        for i in 1..num_threads {
            let cloned_fd = clone_fuse_fd(&self.channel)?;
            // SAFETY: cloned_fd is valid from clone_fuse_fd
            let channel = Channel::new(Arc::new(cloned_fd.into()));
            let reader = FuseReader::new(channel.sender());
            let fs = Arc::clone(&self.filesystem);
            let state = Arc::clone(&self.state);
            let rt = self.runtime.clone();

            handles.push(
                std::thread::Builder::new()
                    .name(format!("fuse-reader-{i}"))
                    .spawn(move || run_reader_loop(reader, fs, state, rt))?,
            );
        }

        // Run thread 0 on the current thread
        let result = run_reader_loop(reader0, fs0, state0, rt0);

        // Signal shutdown and wait for other threads
        self.state.shutdown.store(true, Ordering::SeqCst);

        for handle in handles {
            // Ignore thread panics - we're shutting down anyway
            let _ = handle.join();
        }

        result
    }

    /// Unmount the filesystem.
    pub fn unmount(&mut self) {
        self.mount = None;
    }

    /// Get a notifier for sending notifications to the kernel.
    pub fn notifier(&self) -> Notifier {
        Notifier::new(self.channel.sender())
    }
}

impl<FS: AsyncFilesystem> Drop for AsyncSession<FS> {
    fn drop(&mut self) {
        // Signal shutdown
        self.state.shutdown.store(true, Ordering::SeqCst);

        // Destroy filesystem if not already done
        if !self.state.destroyed.swap(true, Ordering::SeqCst) {
            self.runtime.block_on(self.filesystem.destroy());
        }

        // Unmount if we have a mount
        if let Some((mountpoint, _mount)) = self.mount.take() {
            info!("Unmounting async session at {}", mountpoint.display());
        }
    }
}

/// The main reader loop - reads requests and spawns async handlers.
///
/// This function runs on a single thread and continuously reads FUSE requests
/// from the kernel, dispatching each as an async task.
fn run_reader_loop<FS: AsyncFilesystem>(
    reader: FuseReader,
    filesystem: Arc<FS>,
    state: Arc<SharedSessionState>,
    runtime: tokio::runtime::Handle,
) -> io::Result<()> {
    // Each thread gets its own buffer
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let buf = aligned_sub_buf(&mut buffer, std::mem::align_of::<abi::fuse_in_header>());

    loop {
        // Check for shutdown signal
        if state.shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Read the next request from the kernel
        match reader.read(buf) {
            Ok(size) => {
                let owned_req = reader.make_owned_request(&buf[..size]);

                // Spawn the handler as an async task
                let fs = Arc::clone(&filesystem);
                let st = Arc::clone(&state);

                runtime.spawn(async move {
                    dispatch_async(fs, st, owned_req).await;
                });
            }
            Err(err) => match err.raw_os_error() {
                Some(ENOENT | EINTR | EAGAIN) => continue,
                Some(ENODEV) => break, // Unmounted
                _ => return Err(err),
            },
        }
    }

    Ok(())
}

/// Dispatch a FUSE request asynchronously.
async fn dispatch_async<FS: AsyncFilesystem>(
    fs: Arc<FS>,
    state: Arc<SharedSessionState>,
    owned_req: OwnedRequest,
) {
    use crate::ll::{Operation, Request as _};
    #[cfg(feature = "abi-7-21")]
    use crate::reply::ReplyDirectoryPlus;
    use crate::reply::{Reply, ReplyDirectory, ReplySender};
    use crate::{KernelConfig, PollHandle};
    use std::path::Path;

    let request = match owned_req.parse() {
        Some(r) => r,
        None => {
            error!("Failed to parse FUSE request");
            return;
        }
    };

    let unique = request.unique();
    let sender = owned_req.clone_sender();

    // Create a Request wrapper for the filesystem callbacks
    let req = match owned_req.as_legacy_request() {
        Some(r) => r,
        None => return,
    };

    debug!("Dispatching request: {}", request);

    // Check ACL permissions
    let uid = request.uid();
    let acl_ok = match state.allowed {
        SessionACL::All => true,
        SessionACL::RootAndOwner => uid == state.session_owner || uid == 0,
        SessionACL::Owner => uid == state.session_owner,
    };

    let op = match request.operation() {
        Ok(op) => op,
        Err(_) => {
            let _ = send_error(&sender, unique.into(), libc::ENOSYS);
            return;
        }
    };

    // For certain kernel-initiated operations, skip ACL check
    let skip_acl = matches!(
        op,
        Operation::Init(_)
            | Operation::Destroy(_)
            | Operation::Read(_)
            | Operation::ReadDir(_)
            | Operation::BatchForget(_)
            | Operation::Forget(_)
            | Operation::Write(_)
            | Operation::FSync(_)
            | Operation::FSyncDir(_)
            | Operation::Release(_)
            | Operation::ReleaseDir(_)
    );

    #[cfg(feature = "abi-7-21")]
    let skip_acl = skip_acl || matches!(op, Operation::ReadDirPlus(_));

    if !acl_ok && !skip_acl {
        let _ = send_error(&sender, unique.into(), libc::EACCES);
        return;
    }

    // Check initialization state
    let initialized = state.initialized.load(Ordering::SeqCst);
    let destroyed = state.destroyed.load(Ordering::SeqCst);

    match op {
        Operation::Init(x) => {
            let v = x.version();
            if v < crate::ll::Version(7, 6) {
                error!("Unsupported FUSE ABI version {v}");
                let _ = send_error(&sender, unique.into(), libc::EPROTO);
                return;
            }

            state.proto_major.store(v.major(), Ordering::SeqCst);
            state.proto_minor.store(v.minor(), Ordering::SeqCst);

            let mut config = KernelConfig::new(x.capabilities(), x.max_readahead());

            if let Err(errno) = fs.init(&req, &mut config).await {
                let _ = send_error(&sender, unique.into(), errno);
                return;
            }

            state.initialized.store(true, Ordering::SeqCst);

            let response = x.reply(&config);
            let _ = response.with_iovec(unique, |iov| sender.send(iov));
        }

        _ if !initialized => {
            warn!("Ignoring FUSE operation before init: {}", request);
            let _ = send_error(&sender, unique.into(), libc::EIO);
        }

        Operation::Destroy(x) => {
            fs.destroy().await;
            state.destroyed.store(true, Ordering::SeqCst);
            let response = x.reply();
            let _ = response.with_iovec(unique, |iov| sender.send(iov));
        }

        _ if destroyed => {
            warn!("Ignoring FUSE operation after destroy: {}", request);
            let _ = send_error(&sender, unique.into(), libc::EIO);
        }

        Operation::Lookup(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.lookup(&req, request.nodeid().into(), x.name().as_ref(), reply)
                .await;
        }

        Operation::Forget(x) => {
            fs.forget(&req, request.nodeid().into(), x.nlookup()).await;
            // No reply for forget
        }

        Operation::BatchForget(x) => {
            fs.batch_forget(&req, x.nodes()).await;
            // No reply for batch_forget
        }

        Operation::GetAttr(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.getattr(
                &req,
                request.nodeid().into(),
                x.file_handle().map(Into::into),
                reply,
            )
            .await;
        }

        Operation::SetAttr(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.setattr(
                &req,
                request.nodeid().into(),
                x.mode(),
                x.uid(),
                x.gid(),
                x.size(),
                x.atime(),
                x.mtime(),
                x.ctime(),
                x.file_handle().map(Into::into),
                x.crtime(),
                x.chgtime(),
                x.bkuptime(),
                x.flags(),
                reply,
            )
            .await;
        }

        Operation::ReadLink(_) => {
            let reply = Reply::new(unique.into(), sender);
            fs.readlink(&req, request.nodeid().into(), reply).await;
        }

        Operation::MkNod(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.mknod(
                &req,
                request.nodeid().into(),
                x.name().as_ref(),
                x.mode(),
                x.umask(),
                x.rdev(),
                reply,
            )
            .await;
        }

        Operation::MkDir(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.mkdir(
                &req,
                request.nodeid().into(),
                x.name().as_ref(),
                x.mode(),
                x.umask(),
                reply,
            )
            .await;
        }

        Operation::Unlink(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.unlink(&req, request.nodeid().into(), x.name().as_ref(), reply)
                .await;
        }

        Operation::RmDir(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.rmdir(&req, request.nodeid().into(), x.name().as_ref(), reply)
                .await;
        }

        Operation::SymLink(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.symlink(
                &req,
                request.nodeid().into(),
                x.link_name().as_ref(),
                Path::new(x.target()),
                reply,
            )
            .await;
        }

        Operation::Rename(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.rename(
                &req,
                request.nodeid().into(),
                x.src().name.as_ref(),
                x.dest().dir.into(),
                x.dest().name.as_ref(),
                0,
                reply,
            )
            .await;
        }

        Operation::Link(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.link(
                &req,
                x.inode_no().into(),
                request.nodeid().into(),
                x.dest().name.as_ref(),
                reply,
            )
            .await;
        }

        Operation::Open(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.open(&req, request.nodeid().into(), x.flags(), reply)
                .await;
        }

        Operation::Read(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.read(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.offset(),
                x.size(),
                x.flags(),
                x.lock_owner().map(Into::into),
                reply,
            )
            .await;
        }

        Operation::Write(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.write(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.offset(),
                x.data(),
                x.write_flags(),
                x.flags(),
                x.lock_owner().map(Into::into),
                reply,
            )
            .await;
        }

        Operation::Flush(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.flush(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.lock_owner().into(),
                reply,
            )
            .await;
        }

        Operation::Release(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.release(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.flags(),
                x.lock_owner().map(Into::into),
                x.flush(),
                reply,
            )
            .await;
        }

        Operation::FSync(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.fsync(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.fdatasync(),
                reply,
            )
            .await;
        }

        Operation::OpenDir(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.opendir(&req, request.nodeid().into(), x.flags(), reply)
                .await;
        }

        Operation::ReadDir(x) => {
            let reply = ReplyDirectory::new(unique.into(), sender, x.size() as usize);
            fs.readdir(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.offset(),
                reply,
            )
            .await;
        }

        #[cfg(feature = "abi-7-21")]
        Operation::ReadDirPlus(x) => {
            let reply = ReplyDirectoryPlus::new(unique.into(), sender, x.size() as usize);
            fs.readdirplus(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.offset(),
                reply,
            )
            .await;
        }

        Operation::ReleaseDir(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.releasedir(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.flags(),
                reply,
            )
            .await;
        }

        Operation::FSyncDir(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.fsyncdir(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.fdatasync(),
                reply,
            )
            .await;
        }

        Operation::StatFs(_) => {
            let reply = Reply::new(unique.into(), sender);
            fs.statfs(&req, request.nodeid().into(), reply).await;
        }

        Operation::SetXAttr(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.setxattr(
                &req,
                request.nodeid().into(),
                x.name(),
                x.value(),
                x.flags(),
                x.position(),
                reply,
            )
            .await;
        }

        Operation::GetXAttr(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.getxattr(&req, request.nodeid().into(), x.name(), x.size_u32(), reply)
                .await;
        }

        Operation::ListXAttr(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.listxattr(&req, request.nodeid().into(), x.size(), reply)
                .await;
        }

        Operation::RemoveXAttr(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.removexattr(&req, request.nodeid().into(), x.name(), reply)
                .await;
        }

        Operation::Access(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.access(&req, request.nodeid().into(), x.mask(), reply)
                .await;
        }

        Operation::Create(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.create(
                &req,
                request.nodeid().into(),
                x.name().as_ref(),
                x.mode(),
                x.umask(),
                x.flags(),
                reply,
            )
            .await;
        }

        Operation::GetLk(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.getlk(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.lock_owner().into(),
                x.lock().range.0,
                x.lock().range.1,
                x.lock().typ,
                x.lock().pid,
                reply,
            )
            .await;
        }

        Operation::SetLk(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.setlk(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.lock_owner().into(),
                x.lock().range.0,
                x.lock().range.1,
                x.lock().typ,
                x.lock().pid,
                false,
                reply,
            )
            .await;
        }

        Operation::SetLkW(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.setlk(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.lock_owner().into(),
                x.lock().range.0,
                x.lock().range.1,
                x.lock().typ,
                x.lock().pid,
                true,
                reply,
            )
            .await;
        }

        Operation::BMap(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.bmap(
                &req,
                request.nodeid().into(),
                x.block_size(),
                x.block(),
                reply,
            )
            .await;
        }

        Operation::IoCtl(x) => {
            if x.unrestricted() {
                let _ = send_error(&sender, unique.into(), libc::ENOSYS);
                return;
            }
            let reply = Reply::new(unique.into(), sender);
            fs.ioctl(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.flags(),
                x.command(),
                x.in_data(),
                x.out_size(),
                reply,
            )
            .await;
        }

        Operation::Poll(x) => {
            let ph = PollHandle::new(sender.clone(), x.kernel_handle());
            let reply = Reply::new(unique.into(), sender);
            fs.poll(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                ph,
                x.events(),
                x.flags(),
                reply,
            )
            .await;
        }

        #[cfg(feature = "abi-7-19")]
        Operation::FAllocate(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.fallocate(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.offset(),
                x.len(),
                x.mode(),
                reply,
            )
            .await;
        }

        #[cfg(feature = "abi-7-23")]
        Operation::Rename2(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.rename(
                &req,
                x.from().dir.into(),
                x.from().name.as_ref(),
                x.to().dir.into(),
                x.to().name.as_ref(),
                x.flags(),
                reply,
            )
            .await;
        }

        #[cfg(feature = "abi-7-24")]
        Operation::Lseek(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.lseek(
                &req,
                request.nodeid().into(),
                x.file_handle().into(),
                x.offset(),
                x.whence(),
                reply,
            )
            .await;
        }

        #[cfg(feature = "abi-7-28")]
        Operation::CopyFileRange(x) => {
            use std::convert::TryInto;
            let (i, o) = (x.src(), x.dest());
            let reply = Reply::new(unique.into(), sender);
            fs.copy_file_range(
                &req,
                i.inode.into(),
                i.file_handle.into(),
                i.offset,
                o.inode.into(),
                o.file_handle.into(),
                o.offset,
                x.len(),
                x.flags().try_into().unwrap_or(0),
                reply,
            )
            .await;
        }

        #[cfg(target_os = "macos")]
        Operation::SetVolName(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.setvolname(&req, x.name(), reply).await;
        }

        #[cfg(target_os = "macos")]
        Operation::GetXTimes(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.getxtimes(&req, x.nodeid().into(), reply).await;
        }

        #[cfg(target_os = "macos")]
        Operation::Exchange(x) => {
            let reply = Reply::new(unique.into(), sender);
            fs.exchange(
                &req,
                x.from().dir.into(),
                x.from().name.as_ref(),
                x.to().dir.into(),
                x.to().name.as_ref(),
                x.options(),
                reply,
            )
            .await;
        }

        // Unsupported operations
        Operation::Interrupt(_) | Operation::NotifyReply(_) | Operation::CuseInit(_) => {
            let _ = send_error(&sender, unique.into(), libc::ENOSYS);
        }
    }
}

/// Send an error reply.
fn send_error(sender: &ChannelSender, unique: u64, errno: i32) -> io::Result<()> {
    use crate::reply::ReplySender;

    let header = abi::fuse_out_header {
        len: std::mem::size_of::<abi::fuse_out_header>() as u32,
        error: -errno,
        unique,
    };

    // SAFETY: fuse_out_header is a plain C struct with no padding concerns
    let header_bytes = unsafe {
        std::slice::from_raw_parts(
            &header as *const _ as *const u8,
            std::mem::size_of::<abi::fuse_out_header>(),
        )
    };

    sender.send(&[io::IoSlice::new(header_bytes)])
}

/// Get an aligned sub-buffer from a buffer.
fn aligned_sub_buf(buf: &mut [u8], alignment: usize) -> &mut [u8] {
    let off = alignment - (buf.as_ptr() as usize) % alignment;
    if off == alignment {
        buf
    } else {
        &mut buf[off..]
    }
}

/// Mount a filesystem using the async multi-threaded session.
///
/// # Arguments
///
/// * `filesystem` - The async filesystem implementation
/// * `mountpoint` - Path to mount the filesystem at
/// * `options` - Mount options
/// * `num_threads` - Number of reader threads (recommend 2-4 for NVMe, 1 for slow backends)
/// * `runtime` - Tokio runtime handle
///
/// # Errors
///
/// Returns an error if mounting fails or the session encounters an error.
#[cfg(target_os = "linux")]
pub fn mount_async<FS: AsyncFilesystem, P: AsRef<Path>>(
    filesystem: FS,
    mountpoint: P,
    options: &[MountOption],
    num_threads: usize,
    runtime: tokio::runtime::Handle,
) -> io::Result<()> {
    let session = AsyncSession::new(filesystem, mountpoint, options, runtime)?;
    session.run(num_threads)
}

/// Stub for non-Linux platforms.
#[cfg(not(target_os = "linux"))]
pub fn mount_async<FS: AsyncFilesystem, P: AsRef<Path>>(
    _filesystem: FS,
    _mountpoint: P,
    _options: &[crate::MountOption],
    _num_threads: usize,
    _runtime: tokio::runtime::Handle,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Async FUSE mount only supported on Linux",
    ))
}
