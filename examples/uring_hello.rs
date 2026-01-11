//! Simple test for FUSE io_uring support

use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};

const TTL: Duration = Duration::from_secs(1);
const BLOCK_SIZE: u64 = 512;

const ROOT_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH,
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 0,
    gid: 0,
    rdev: 0,
    blksize: BLOCK_SIZE as u32,
    flags: 0,
};

const HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 13,
    blocks: 1,
    atime: UNIX_EPOCH,
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 0,
    gid: 0,
    rdev: 0,
    blksize: BLOCK_SIZE as u32,
    flags: 0,
};

const HELLO_CONTENT: &str = "Hello World!\n";

struct HelloFS;

impl Filesystem for HelloFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 && name.to_str() == Some("hello.txt") {
            reply.entry(&TTL, &HELLO_TXT_ATTR, 0);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match ino {
            1 => reply.attr(&TTL, &ROOT_DIR_ATTR),
            2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
            _ => reply.error(libc::ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        if ino == 2 {
            let content = HELLO_CONTENT.as_bytes();
            let offset = offset as usize;
            let size = size as usize;
            if offset < content.len() {
                let end = std::cmp::min(offset + size, content.len());
                reply.data(&content[offset..end]);
            } else {
                reply.data(&[]);
            }
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(libc::ENOENT);
            return;
        }

        let entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
            (2, FileType::RegularFile, "hello.txt"),
        ];

        for (i, (ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*ino, (i + 1) as i64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }
}

fn main() {
    env_logger::init();

    let mountpoint = std::env::args()
        .nth(1)
        .expect("Usage: uring_hello <mountpoint>");
    let mountpoint = std::path::Path::new(&mountpoint);

    println!("Testing FUSE io_uring at {}", mountpoint.display());

    // Try io_uring mount
    match fuser::mount_uring(
        HelloFS,
        mountpoint,
        &[MountOption::AllowOther],
        1, // single thread for testing
    ) {
        Ok(()) => {
            println!("io_uring session completed successfully!");
        }
        Err(e) => {
            eprintln!("io_uring mount failed: {}", e);
            eprintln!("Error kind: {:?}", e.kind());
            std::process::exit(1);
        }
    }
}
