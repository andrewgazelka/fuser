//! FUSE io_uring session implementation.
//!
//! This module provides a high-performance FUSE session using Linux's io_uring
//! interface (available in Linux 6.14+). It eliminates syscall overhead by using
//! shared memory rings for communication with the kernel.
//!
//! # Architecture
//!
//! - One io_uring queue per CPU core for NUMA-aware operation
//! - Uses `IORING_OP_URING_CMD` with 80-byte command area for FUSE ops
//! - Zero-copy buffer passing between kernel and userspace
//! - Commit-and-fetch pattern: reply to one request and fetch next atomically

#![cfg(all(target_os = "linux", feature = "io-uring"))]

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::JoinHandle;

use io_uring::{IoUring, opcode, squeue, types};
use log::{debug, error, info, warn};
use nix::unistd::geteuid;

use crate::Filesystem;
#[cfg(any(
    fuser_mount_impl = "pure-rust",
    fuser_mount_impl = "libfuse2",
    fuser_mount_impl = "libfuse3"
))]
use crate::MountOption;
#[cfg(any(
    fuser_mount_impl = "pure-rust",
    fuser_mount_impl = "libfuse2",
    fuser_mount_impl = "libfuse3"
))]
use crate::mnt::Mount;
use crate::session::{MAX_WRITE_SIZE, SessionACL};

// =============================================================================
// FUSE io_uring kernel structures (from fuse_kernel.h)
// =============================================================================

/// Size of the in/out header area in FUSE uring requests.
const FUSE_URING_IN_OUT_HEADER_SZ: usize = 128;

/// Size of the op-specific header area.
const FUSE_URING_OP_IN_OUT_SZ: usize = 128;

/// FUSE io_uring command types.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuseUringCmd {
    /// Invalid command (placeholder).
    Invalid = 0,
    /// Register a buffer and fetch a FUSE request.
    Register = 1,
    /// Commit a response and fetch the next request atomically.
    CommitAndFetch = 2,
}

/// Entry in/out metadata for FUSE io_uring.
///
/// This structure is part of the request header and contains
/// per-entry metadata like commit ID and payload size.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct FuseUringEntInOut {
    /// Flags (reserved).
    pub flags: u64,
    /// Commit ID for replies.
    pub commit_id: u64,
    /// Size of the payload buffer.
    pub payload_sz: u32,
    /// Padding.
    pub padding: u32,
    /// Reserved.
    pub reserved: u64,
}

/// Header for FUSE io_uring requests.
///
/// Contains the in/out header, op-specific data, and entry metadata.
#[repr(C)]
#[derive(Clone)]
pub struct FuseUringReqHeader {
    /// fuse_in_header / fuse_out_header.
    pub in_out: [u8; FUSE_URING_IN_OUT_HEADER_SZ],
    /// Op-specific header data.
    pub op_in: [u8; FUSE_URING_OP_IN_OUT_SZ],
    /// Entry in/out metadata.
    pub ring_ent_in_out: FuseUringEntInOut,
}

impl Default for FuseUringReqHeader {
    fn default() -> Self {
        Self {
            in_out: [0u8; FUSE_URING_IN_OUT_HEADER_SZ],
            op_in: [0u8; FUSE_URING_OP_IN_OUT_SZ],
            ring_ent_in_out: FuseUringEntInOut::default(),
        }
    }
}

/// Command data in the 80-byte SQE command area.
///
/// This is placed in the io_uring SQE's cmd field when using URING_CMD.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct FuseUringCmdReq {
    /// Flags (reserved).
    pub flags: u64,
    /// Commit ID for commit-and-fetch.
    pub commit_id: u64,
    /// Queue ID (CPU index).
    pub qid: u16,
    /// Padding.
    pub padding: [u8; 6],
}

impl FuseUringCmdReq {
    /// Serialize to 80-byte command array.
    pub fn to_cmd_bytes(&self) -> [u8; 80] {
        let mut cmd = [0u8; 80];
        cmd[0..8].copy_from_slice(&self.flags.to_ne_bytes());
        cmd[8..16].copy_from_slice(&self.commit_id.to_ne_bytes());
        cmd[16..18].copy_from_slice(&self.qid.to_ne_bytes());
        // padding bytes 18..24 already zeroed
        cmd
    }
}

// =============================================================================
// Queue entry and ring structures
// =============================================================================

/// A single entry in the FUSE io_uring queue.
///
/// Each entry represents one request slot with its own header and payload buffers.
/// The iovecs are stored inline to ensure they remain valid for kernel access.
pub struct RingEntry {
    /// Request header buffer (boxed for stable address).
    pub header: Box<FuseUringReqHeader>,
    /// Payload buffer for request/response data.
    pub payload: Vec<u8>,
    /// Cached iovecs pointing to header and payload (stable addresses).
    /// Must be updated if header/payload are reallocated (they aren't in normal operation).
    pub iovecs: [libc::iovec; 2],
    /// Current commit ID (set by kernel on fetch).
    pub commit_id: u64,
    /// Last command issued for this entry.
    pub last_cmd: FuseUringCmd,
}

impl RingEntry {
    /// Create a new ring entry with the given payload capacity.
    pub fn new(payload_capacity: usize) -> Self {
        let mut header = Box::new(FuseUringReqHeader::default());
        let mut payload = vec![0u8; payload_capacity];

        // Pre-compute iovecs with stable pointers
        let iovecs = [
            libc::iovec {
                iov_base: header.as_mut() as *mut _ as *mut libc::c_void,
                iov_len: std::mem::size_of::<FuseUringReqHeader>(),
            },
            libc::iovec {
                iov_base: payload.as_mut_ptr() as *mut libc::c_void,
                iov_len: payload.len(),
            },
        ];

        Self {
            header,
            payload,
            iovecs,
            commit_id: 0,
            last_cmd: FuseUringCmd::Invalid,
        }
    }

    /// Get pointer to the iovecs array.
    #[inline]
    pub fn iovecs_ptr(&self) -> u64 {
        self.iovecs.as_ptr() as u64
    }
}

/// Configuration for RingQueue optimizations.
#[derive(Debug, Clone, Copy)]
pub struct RingQueueConfig {
    /// Use kernel-side SQ polling (IORING_SETUP_SQPOLL).
    /// This eliminates submit syscalls but uses CPU for polling.
    pub sqpoll: bool,
    /// Idle timeout in milliseconds for SQPOLL (0 = never idle).
    pub sqpoll_idle_ms: u32,
    /// Pin the queue to a specific CPU for cache locality.
    pub pin_cpu: Option<usize>,
}

impl Default for RingQueueConfig {
    fn default() -> Self {
        Self {
            sqpoll: false,
            sqpoll_idle_ms: 0,
            pin_cpu: None,
        }
    }
}

/// A per-CPU io_uring queue for FUSE operations.
pub struct RingQueue {
    /// Queue ID (CPU index).
    pub qid: usize,
    /// The io_uring instance (with 128-byte SQEs for URING_CMD).
    pub ring: IoUring<squeue::Entry128>,
    /// Ring entries (one per queue depth slot).
    pub entries: Vec<RingEntry>,
    /// File descriptor for /dev/fuse (registered with io_uring).
    pub fuse_fd: RawFd,
    /// Whether SQPOLL mode is enabled.
    sqpoll: bool,
}

impl RingQueue {
    /// Create a new ring queue with default configuration.
    pub fn new(qid: usize, depth: usize, payload_sz: usize, fuse_fd: RawFd) -> io::Result<Self> {
        Self::with_config(qid, depth, payload_sz, fuse_fd, RingQueueConfig::default())
    }

    /// Create a new ring queue with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `qid` - Queue ID (CPU index)
    /// * `depth` - Number of entries in the queue
    /// * `payload_sz` - Size of payload buffers
    /// * `fuse_fd` - /dev/fuse file descriptor
    /// * `config` - Optimization configuration
    pub fn with_config(
        qid: usize,
        depth: usize,
        payload_sz: usize,
        fuse_fd: RawFd,
        config: RingQueueConfig,
    ) -> io::Result<Self> {
        // Create io_uring with SQE128 support (Entry128 automatically sets IORING_SETUP_SQE128)
        let mut builder = IoUring::<squeue::Entry128>::builder();

        // Set CQ size to 2x depth to avoid overflow
        builder.setup_cqsize((depth * 2) as u32);

        // SQPOLL mode: kernel polls the SQ, eliminating submit syscalls
        if config.sqpoll {
            builder.setup_sqpoll(config.sqpoll_idle_ms);
            // Attach to specific CPU if requested
            if let Some(cpu) = config.pin_cpu {
                builder.setup_sqpoll_cpu(cpu as u32);
            }
        }

        let ring = builder.build((depth + 1) as u32)?; // +1 for potential eventfd

        // Register /dev/fuse fd with io_uring for IOSQE_FIXED_FILE
        ring.submitter().register_files(&[fuse_fd])?;

        // Create ring entries with stable buffer addresses
        let entries = (0..depth).map(|_| RingEntry::new(payload_sz)).collect();

        Ok(Self {
            qid,
            ring,
            entries,
            fuse_fd,
            sqpoll: config.sqpoll,
        })
    }

    /// Queue a REGISTER command for an entry (does not submit yet).
    ///
    /// This registers the entry's buffers with the kernel and requests
    /// the first FUSE request to be placed there. Call `submit()` to
    /// actually submit queued entries.
    #[inline]
    pub fn queue_register(&mut self, entry_idx: usize) -> io::Result<()> {
        let entry = &mut self.entries[entry_idx];
        entry.last_cmd = FuseUringCmd::Register;

        // Use cached iovec pointer (stable address)
        let iov_ptr = entry.iovecs_ptr();

        let cmd_req = FuseUringCmdReq {
            flags: 0,
            commit_id: 0, // Not needed for register
            qid: self.qid as u16,
            padding: [0; 6],
        };

        // Build UringCmd80 with FUSE_IO_URING_CMD_REGISTER
        let sqe = opcode::UringCmd80::new(types::Fixed(0), FuseUringCmd::Register as u32)
            .cmd(cmd_req.to_cmd_bytes())
            .addr(Some(iov_ptr))
            .build()
            .user_data(entry_idx as u64);

        unsafe {
            self.ring.submission().push(&sqe)?;
        }

        Ok(())
    }

    /// Queue a COMMIT_AND_FETCH command for an entry (does not submit yet).
    ///
    /// This commits the response in the entry's buffers and atomically
    /// fetches the next request. Call `submit()` to actually submit.
    #[inline(always)]
    pub fn queue_commit_and_fetch(&mut self, entry_idx: usize) -> io::Result<()> {
        let entry = &mut self.entries[entry_idx];
        entry.last_cmd = FuseUringCmd::CommitAndFetch;

        let cmd_req = FuseUringCmdReq {
            flags: 0,
            commit_id: entry.commit_id,
            qid: self.qid as u16,
            padding: [0; 6],
        };

        let sqe = opcode::UringCmd80::new(types::Fixed(0), FuseUringCmd::CommitAndFetch as u32)
            .cmd(cmd_req.to_cmd_bytes())
            .build()
            .user_data(entry_idx as u64);

        unsafe {
            self.ring.submission().push(&sqe)?;
        }

        Ok(())
    }

    /// Submit all queued SQEs to the kernel.
    ///
    /// In SQPOLL mode, this is a no-op since the kernel polls the SQ.
    #[inline(always)]
    pub fn submit(&mut self) -> io::Result<usize> {
        if self.sqpoll {
            // In SQPOLL mode, kernel polls the SQ - no syscall needed
            // But we need to check if the kernel is sleeping and wake it
            self.ring.submission().sync();
            Ok(0)
        } else {
            self.ring.submit()
        }
    }

    /// Submit all pending SQEs and wait for at least one completion.
    #[inline(always)]
    pub fn submit_and_wait(&mut self, min_complete: usize) -> io::Result<usize> {
        self.ring.submit_and_wait(min_complete)
    }

    /// Check if there are pending CQEs without blocking.
    #[inline(always)]
    pub fn has_completions(&self) -> bool {
        !self.ring.completion().is_empty()
    }

    /// Drain completion queue entries into the provided buffer.
    ///
    /// Returns the number of completions. Each completion is (entry_idx, result).
    /// The buffer is cleared and reused to avoid allocation.
    #[inline(always)]
    pub fn drain_completions(&mut self, buffer: &mut Vec<(usize, i32)>) -> usize {
        buffer.clear();

        // Sync completion queue to see latest completions
        self.ring.completion().sync();

        for cqe in self.ring.completion() {
            buffer.push((cqe.user_data() as usize, cqe.result()));
        }

        buffer.len()
    }

    /// Get mutable reference to an entry.
    #[inline]
    pub fn entry_mut(&mut self, idx: usize) -> &mut RingEntry {
        &mut self.entries[idx]
    }

    /// Get the queue depth (number of entries).
    #[inline]
    pub fn depth(&self) -> usize {
        self.entries.len()
    }
}

// =============================================================================
// Session state
// =============================================================================

/// Shared state for the io_uring FUSE session.
#[derive(Debug)]
pub struct SharedSessionState {
    /// Access control list.
    pub allowed: SessionACL,
    /// User that launched the process.
    pub session_owner: u32,
    /// FUSE protocol major version.
    pub proto_major: AtomicU32,
    /// FUSE protocol minor version.
    pub proto_minor: AtomicU32,
    /// Whether the filesystem is initialized.
    pub initialized: AtomicBool,
    /// Whether the filesystem was destroyed.
    pub destroyed: AtomicBool,
    /// Shutdown signal.
    pub shutdown: AtomicBool,
}

impl SharedSessionState {
    /// Create new shared state.
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

// =============================================================================
// io_uring Session
// =============================================================================

/// Placeholder for mount when none is available.
#[cfg(fuser_mount_impl = "none")]
#[derive(Debug)]
pub struct NoMount;

/// High-performance FUSE session using io_uring.
///
/// This session type uses Linux's io_uring interface for zero-copy,
/// syscall-free FUSE operations. Available on Linux 6.14+.
pub struct UringSession<FS: Filesystem + Send + Sync> {
    /// Filesystem implementation.
    filesystem: Arc<FS>,
    /// /dev/fuse file descriptor.
    fuse_fd: Arc<OwnedFd>,
    /// Shared session state.
    state: Arc<SharedSessionState>,
    /// Mount handle (keeps filesystem mounted).
    #[cfg(any(
        fuser_mount_impl = "pure-rust",
        fuser_mount_impl = "libfuse2",
        fuser_mount_impl = "libfuse3"
    ))]
    mount: Option<(PathBuf, Mount)>,
    #[cfg(fuser_mount_impl = "none")]
    mount: Option<(PathBuf, NoMount)>,
}

impl<FS: Filesystem + Send + Sync + 'static> UringSession<FS> {
    /// Create a new io_uring session by mounting at the given path.
    #[cfg(any(
        fuser_mount_impl = "pure-rust",
        fuser_mount_impl = "libfuse2",
        fuser_mount_impl = "libfuse3"
    ))]
    pub fn new<P: AsRef<Path>>(
        filesystem: FS,
        mountpoint: P,
        options: &[MountOption],
    ) -> io::Result<Self> {
        let mountpoint = mountpoint.as_ref();
        info!("Mounting io_uring FUSE at {}", mountpoint.display());

        // Handle AutoUnmount
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

        let allowed = if options.contains(&MountOption::AllowRoot) {
            SessionACL::RootAndOwner
        } else if options.contains(&MountOption::AllowOther) {
            SessionACL::All
        } else {
            SessionACL::Owner
        };

        // Convert Arc<OwnedFd> to Arc<OwnedFd> (file is already Arc<std::fs::File>)
        let raw_fd = file.as_raw_fd();
        let fuse_fd = unsafe { OwnedFd::from_raw_fd(libc::dup(raw_fd)) };

        Ok(Self {
            filesystem: Arc::new(filesystem),
            fuse_fd: Arc::new(fuse_fd),
            state: Arc::new(SharedSessionState::new(allowed)),
            mount: Some((mountpoint.to_owned(), mount)),
        })
    }

    /// Run the session with the specified number of worker threads.
    ///
    /// Each thread gets its own io_uring queue for NUMA-aware operation.
    /// If `num_threads` is 0, uses the number of CPU cores.
    pub fn run(self, num_threads: usize) -> io::Result<()> {
        let num_threads = if num_threads == 0 {
            num_cpus()
        } else {
            num_threads
        };

        info!(
            "Starting io_uring FUSE session with {} worker threads",
            num_threads
        );

        let payload_sz = MAX_WRITE_SIZE;
        let queue_depth = 32; // Entries per queue

        if num_threads == 1 {
            // Single-threaded mode
            let mut queue = RingQueue::new(0, queue_depth, payload_sz, self.fuse_fd.as_raw_fd())?;
            return run_queue_loop(
                &mut queue,
                Arc::clone(&self.filesystem),
                Arc::clone(&self.state),
            );
        }

        // Multi-threaded mode
        let mut handles: Vec<JoinHandle<io::Result<()>>> = Vec::with_capacity(num_threads);

        for qid in 0..num_threads {
            let fs = Arc::clone(&self.filesystem);
            let state = Arc::clone(&self.state);
            let fuse_fd = self.fuse_fd.as_raw_fd();

            handles.push(
                std::thread::Builder::new()
                    .name(format!("fuse-uring-{qid}"))
                    .spawn(move || {
                        let mut queue = RingQueue::new(qid, queue_depth, payload_sz, fuse_fd)?;
                        run_queue_loop(&mut queue, fs, state)
                    })?,
            );
        }

        // Wait for all threads
        for handle in handles {
            if let Err(e) = handle.join() {
                error!("Worker thread panicked: {:?}", e);
            }
        }

        Ok(())
    }

    /// Unmount the filesystem.
    pub fn unmount(&mut self) {
        self.mount = None;
    }
}

impl<FS: Filesystem + Send + Sync> Drop for UringSession<FS> {
    fn drop(&mut self) {
        self.state.shutdown.store(true, Ordering::SeqCst);

        if !self.state.destroyed.swap(true, Ordering::SeqCst) {
            self.filesystem.destroy();
        }

        if let Some((mountpoint, _mount)) = self.mount.take() {
            info!("Unmounting io_uring session at {}", mountpoint.display());
        }
    }
}

// =============================================================================
// Queue event loop
// =============================================================================

/// Maximum queue depth we support (for stack allocation).
const MAX_QUEUE_DEPTH: usize = 64;

/// Run the event loop for a single queue.
///
/// This is the hot path - optimized for minimal allocation and syscalls.
fn run_queue_loop<FS: Filesystem + Send + Sync>(
    queue: &mut RingQueue,
    _filesystem: Arc<FS>,
    state: Arc<SharedSessionState>,
) -> io::Result<()> {
    let depth = queue.depth();
    let qid = queue.qid;

    assert!(depth <= MAX_QUEUE_DEPTH, "Queue depth exceeds maximum");

    // Register all entries with the kernel (batch submission)
    for i in 0..depth {
        queue.queue_register(i)?;
    }
    queue.submit()?;

    debug!("Queue {} registered {} entries", qid, depth);

    // Pre-allocate completion buffer (reused every iteration)
    let mut completions: Vec<(usize, i32)> = Vec::with_capacity(depth);
    let mut queued_count = 0usize;

    loop {
        // Check shutdown flag with relaxed ordering (we'll see it eventually)
        if state.shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Wait for at least one completion
        match queue.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) if e.raw_os_error() == Some(libc::ENODEV) => break,
            Err(e) => return Err(e),
        }

        // Drain all available completions into pre-allocated buffer
        queue.drain_completions(&mut completions);

        // Reset queue counter
        queued_count = 0;

        // Process each completion
        for &(entry_idx, result) in &completions {
            if result < 0 {
                let errno = -result;
                if errno == libc::ENODEV {
                    // Unmounted - will exit on next iteration
                    continue;
                }
                error!(
                    "Queue {} entry {} failed: {}",
                    qid,
                    entry_idx,
                    io::Error::from_raw_os_error(errno)
                );
                continue;
            }

            // SAFETY: entry_idx is from kernel, should be valid
            let entry = queue.entry_mut(entry_idx);

            // Update commit ID from kernel response (needed for reply)
            entry.commit_id = entry.header.ring_ent_in_out.commit_id;

            // TODO: Parse fuse_in_header and dispatch to filesystem
            // For now, return ENOSYS for all operations

            // Set error response in fuse_out_header
            // Layout: len (u32), error (i32), unique (u64)
            entry.header.in_out[4..8].copy_from_slice(&(-libc::ENOSYS as i32).to_ne_bytes());
            entry.header.ring_ent_in_out.payload_sz = 0;

            // Queue commit-and-fetch (we'll batch submit at end)
            if queue.queue_commit_and_fetch(entry_idx).is_ok() {
                queued_count += 1;
            }
        }

        // Batch submit all queued operations (single syscall)
        if queued_count > 0 {
            let _ = queue.submit();
        }
    }

    Ok(())
}

/// Get the number of CPU cores.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

// =============================================================================
// Public mount function
// =============================================================================

/// Mount a filesystem using io_uring for high-performance operation.
///
/// This requires Linux 6.14+ with FUSE io_uring support enabled.
///
/// # Arguments
///
/// * `filesystem` - The filesystem implementation
/// * `mountpoint` - Path to mount at
/// * `options` - Mount options
/// * `num_threads` - Number of worker threads (0 = one per CPU)
#[cfg(any(
    fuser_mount_impl = "pure-rust",
    fuser_mount_impl = "libfuse2",
    fuser_mount_impl = "libfuse3"
))]
pub fn mount_uring<FS, P>(
    filesystem: FS,
    mountpoint: P,
    options: &[MountOption],
    num_threads: usize,
) -> io::Result<()>
where
    FS: Filesystem + Send + Sync + 'static,
    P: AsRef<Path>,
{
    let session = UringSession::new(filesystem, mountpoint, options)?;
    session.run(num_threads)
}
