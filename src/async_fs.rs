//! Async filesystem trait for multi-threaded FUSE operations.
//!
//! This module provides an async alternative to the sync `Filesystem` trait,
//! enabling concurrent request processing with tokio.

use std::ffi::OsStr;
use std::future::Future;
use std::path::Path;
use std::time::SystemTime;

use libc::c_int;

use crate::reply::{
    ReplyAttr, ReplyBmap, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty,
    ReplyEntry, ReplyIoctl, ReplyLock, ReplyLseek, ReplyOpen, ReplyPoll, ReplyStatfs, ReplyWrite,
    ReplyXattr,
};
use crate::{KernelConfig, PollHandle, Request, TimeOrNow, fuse_forget_one};

#[cfg(target_os = "macos")]
use crate::reply::ReplyXTimes;

/// Async filesystem trait.
///
/// This trait uses `&self` instead of `&mut self`, allowing concurrent access
/// from multiple reader threads. Implementations must handle their own
/// synchronization (e.g., using `Arc<RwLock<...>>` or lock-free data structures).
///
/// All methods return `impl Future` to enable async/await in handlers.
#[allow(clippy::too_many_arguments)]
pub trait AsyncFilesystem: Send + Sync + 'static {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    fn init(
        &self,
        _req: &Request<'_>,
        _config: &mut KernelConfig,
    ) -> impl Future<Output = Result<(), c_int>> + Send {
        async { Ok(()) }
    }

    /// Clean up filesystem.
    /// Called on filesystem exit.
    fn destroy(&self) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Look up a directory entry by name and get its attributes.
    fn lookup(
        &self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEntry,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] lookup(parent: {parent:#x?}, name {name:?})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Forget about an inode.
    fn forget(
        &self,
        _req: &Request<'_>,
        _ino: u64,
        _nlookup: u64,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Batch forget multiple inodes.
    fn batch_forget(
        &self,
        req: &Request<'_>,
        nodes: &[fuse_forget_one],
    ) -> impl Future<Output = ()> + Send {
        async move {
            for node in nodes {
                self.forget(req, node.nodeid, node.nlookup).await;
            }
        }
    }

    /// Get file attributes.
    fn getattr(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: Option<u64>,
        reply: ReplyAttr,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] getattr(ino: {ino:#x?}, fh: {fh:#x?})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Set file attributes.
    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] setattr(ino: {ino:#x?}, mode: {mode:?}, uid: {uid:?}, \
                gid: {gid:?}, size: {size:?}, fh: {fh:?}, flags: {flags:?})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Read symbolic link.
    fn readlink(
        &self,
        _req: &Request<'_>,
        ino: u64,
        reply: ReplyData,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] readlink(ino: {ino:#x?})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Create file node.
    fn mknod(
        &self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] mknod(parent: {parent:#x?}, name: {name:?}, \
                mode: {mode}, umask: {umask:#x?}, rdev: {rdev})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Create a directory.
    fn mkdir(
        &self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] mkdir(parent: {parent:#x?}, name: {name:?}, mode: {mode}, umask: {umask:#x?})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Remove a file.
    fn unlink(
        &self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] unlink(parent: {parent:#x?}, name: {name:?})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Remove a directory.
    fn rmdir(
        &self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] rmdir(parent: {parent:#x?}, name: {name:?})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Create a symbolic link.
    fn symlink(
        &self,
        _req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] symlink(parent: {parent:#x?}, link_name: {link_name:?}, target: {target:?})"
            );
            reply.error(libc::EPERM);
        }
    }

    /// Rename a file.
    fn rename(
        &self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] rename(parent: {parent:#x?}, name: {name:?}, \
                newparent: {newparent:#x?}, newname: {newname:?}, flags: {flags})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Create a hard link.
    fn link(
        &self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] link(ino: {ino:#x?}, newparent: {newparent:#x?}, newname: {newname:?})"
            );
            reply.error(libc::EPERM);
        }
    }

    /// Open a file.
    fn open(
        &self,
        _req: &Request<'_>,
        _ino: u64,
        _flags: i32,
        reply: ReplyOpen,
    ) -> impl Future<Output = ()> + Send {
        async move {
            reply.opened(0, 0);
        }
    }

    /// Read data.
    #[allow(clippy::too_many_arguments)]
    fn read(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] read(ino: {ino:#x?}, fh: {fh}, offset: {offset}, \
                size: {size}, flags: {flags:#x?}, lock_owner: {lock_owner:?})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Write data.
    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] write(ino: {ino:#x?}, fh: {fh}, offset: {offset}, \
                data.len(): {}, write_flags: {write_flags:#x?}, flags: {flags:#x?}, \
                lock_owner: {lock_owner:?})",
                data.len()
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Flush method.
    fn flush(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] flush(ino: {ino:#x?}, fh: {fh}, lock_owner: {lock_owner:?})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Release an open file.
    fn release(
        &self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            reply.ok();
        }
    }

    /// Synchronize file contents.
    fn fsync(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] fsync(ino: {ino:#x?}, fh: {fh}, datasync: {datasync})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Open a directory.
    fn opendir(
        &self,
        _req: &Request<'_>,
        _ino: u64,
        _flags: i32,
        reply: ReplyOpen,
    ) -> impl Future<Output = ()> + Send {
        async move {
            reply.opened(0, 0);
        }
    }

    /// Read directory.
    fn readdir(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectory,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] readdir(ino: {ino:#x?}, fh: {fh}, offset: {offset})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Read directory with attributes.
    fn readdirplus(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectoryPlus,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] readdirplus(ino: {ino:#x?}, fh: {fh}, offset: {offset})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Release an open directory.
    fn releasedir(
        &self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            reply.ok();
        }
    }

    /// Synchronize directory contents.
    fn fsyncdir(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] fsyncdir(ino: {ino:#x?}, fh: {fh}, datasync: {datasync})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Get file system statistics.
    fn statfs(
        &self,
        _req: &Request<'_>,
        _ino: u64,
        reply: ReplyStatfs,
    ) -> impl Future<Output = ()> + Send {
        async move {
            reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
        }
    }

    /// Set an extended attribute.
    fn setxattr(
        &self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        _value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] setxattr(ino: {ino:#x?}, name: {name:?}, \
                flags: {flags:#x?}, position: {position})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Get an extended attribute.
    fn getxattr(
        &self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] getxattr(ino: {ino:#x?}, name: {name:?}, size: {size})");
            reply.error(libc::ENOSYS);
        }
    }

    /// List extended attribute names.
    fn listxattr(
        &self,
        _req: &Request<'_>,
        ino: u64,
        size: u32,
        reply: ReplyXattr,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] listxattr(ino: {ino:#x?}, size: {size})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Remove an extended attribute.
    fn removexattr(
        &self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] removexattr(ino: {ino:#x?}, name: {name:?})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Check file access permissions.
    fn access(
        &self,
        _req: &Request<'_>,
        ino: u64,
        mask: i32,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] access(ino: {ino:#x?}, mask: {mask})");
            reply.error(libc::ENOSYS);
        }
    }

    /// Create and open a file.
    fn create(
        &self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] create(parent: {parent:#x?}, name: {name:?}, mode: {mode}, \
                umask: {umask:#x?}, flags: {flags:#x?})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Test for a POSIX file lock.
    #[allow(clippy::too_many_arguments)]
    fn getlk(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] getlk(ino: {ino:#x?}, fh: {fh}, lock_owner: {lock_owner}, \
                start: {start}, end: {end}, typ: {typ}, pid: {pid})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Acquire, modify or release a POSIX file lock.
    #[allow(clippy::too_many_arguments)]
    fn setlk(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] setlk(ino: {ino:#x?}, fh: {fh}, lock_owner: {lock_owner}, \
                start: {start}, end: {end}, typ: {typ}, pid: {pid}, sleep: {sleep})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Map block index within file to block index within device.
    fn bmap(
        &self,
        _req: &Request<'_>,
        ino: u64,
        blocksize: u32,
        idx: u64,
        reply: ReplyBmap,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] bmap(ino: {ino:#x?}, blocksize: {blocksize}, idx: {idx})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Control device.
    #[allow(clippy::too_many_arguments)]
    fn ioctl(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: ReplyIoctl,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] ioctl(ino: {ino:#x?}, fh: {fh}, flags: {flags}, \
                cmd: {cmd}, in_data.len(): {}, out_size: {out_size})",
                in_data.len()
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Poll for events.
    fn poll(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        ph: PollHandle,
        events: u32,
        flags: u32,
        reply: ReplyPoll,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] poll(ino: {ino:#x?}, fh: {fh}, \
                ph: {ph:?}, events: {events}, flags: {flags})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Preallocate or deallocate space to a file.
    fn fallocate(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] fallocate(ino: {ino:#x?}, fh: {fh}, \
                offset: {offset}, length: {length}, mode: {mode})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Reposition read/write file offset.
    fn lseek(
        &self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] lseek(ino: {ino:#x?}, fh: {fh}, \
                offset: {offset}, whence: {whence})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// Copy the specified range from the source inode to the destination inode.
    #[allow(clippy::too_many_arguments)]
    fn copy_file_range(
        &self,
        _req: &Request<'_>,
        ino_in: u64,
        fh_in: u64,
        offset_in: i64,
        ino_out: u64,
        fh_out: u64,
        offset_out: i64,
        len: u64,
        flags: u32,
        reply: ReplyWrite,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] copy_file_range(ino_in: {ino_in:#x?}, fh_in: {fh_in}, \
                offset_in: {offset_in}, ino_out: {ino_out:#x?}, fh_out: {fh_out}, \
                offset_out: {offset_out}, len: {len}, flags: {flags})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// macOS only: Rename the volume.
    #[cfg(target_os = "macos")]
    fn setvolname(
        &self,
        _req: &Request<'_>,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] setvolname(name: {name:?})");
            reply.error(libc::ENOSYS);
        }
    }

    /// macOS only: Exchange data between files.
    #[cfg(target_os = "macos")]
    fn exchange(
        &self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        options: u64,
        reply: ReplyEmpty,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!(
                "[Not Implemented] exchange(parent: {parent:#x?}, name: {name:?}, \
                newparent: {newparent:#x?}, newname: {newname:?}, options: {options})"
            );
            reply.error(libc::ENOSYS);
        }
    }

    /// macOS only: Query extended times.
    #[cfg(target_os = "macos")]
    fn getxtimes(
        &self,
        _req: &Request<'_>,
        ino: u64,
        reply: ReplyXTimes,
    ) -> impl Future<Output = ()> + Send {
        async move {
            log::warn!("[Not Implemented] getxtimes(ino: {ino:#x?})");
            reply.error(libc::ENOSYS);
        }
    }
}
