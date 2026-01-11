//! Owned FUSE request for async dispatch.
//!
//! When dispatching requests asynchronously, we need to own the request data
//! since the original buffer will be reused for the next request.

use std::sync::Arc;

use crate::Request;
use crate::channel::ChannelSender;
use crate::ll::AnyRequest;

/// An owned FUSE request that can be sent across threads.
///
/// This wraps the raw request data in an `Arc` to avoid cloning for each field access,
/// while still allowing the data to be shared across async task boundaries.
#[derive(Debug)]
pub struct OwnedRequest {
    /// The owned request data
    data: Arc<[u8]>,
    /// Channel sender for replies
    sender: ChannelSender,
}

impl OwnedRequest {
    /// Create a new owned request from borrowed data.
    ///
    /// This clones the data into an `Arc<[u8]>` for efficient sharing.
    #[inline]
    pub fn new(data: &[u8], sender: ChannelSender) -> Self {
        Self {
            data: Arc::from(data),
            sender,
        }
    }

    /// Parse the request header.
    ///
    /// Returns `None` if the request data is invalid.
    #[inline]
    pub fn parse(&self) -> Option<AnyRequest<'_>> {
        AnyRequest::try_from(self.data.as_ref()).ok()
    }

    /// Get the raw request data.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get a reference to the channel sender.
    #[inline]
    pub fn sender(&self) -> &ChannelSender {
        &self.sender
    }

    /// Clone the channel sender.
    #[inline]
    pub fn clone_sender(&self) -> ChannelSender {
        self.sender.clone()
    }

    /// Create a legacy `Request` wrapper for filesystem callbacks.
    ///
    /// Returns `None` if the request data is invalid.
    #[inline]
    pub fn as_legacy_request(&self) -> Option<Request<'_>> {
        Request::new(self.sender.clone(), &self.data)
    }
}

// Safety: OwnedRequest is Send because:
// - Arc<[u8]> is Send (immutable shared data)
// - ChannelSender is Send (wraps Arc<File>)
unsafe impl Send for OwnedRequest {}

// Safety: OwnedRequest is Sync because:
// - Arc<[u8]> is Sync (immutable shared data)
// - ChannelSender is Sync (wraps Arc<File>, only does atomic operations)
unsafe impl Sync for OwnedRequest {}

/// A reader that can receive FUSE requests from the kernel.
///
/// Each reader owns a file descriptor (original or cloned) and can
/// independently read requests from the kernel.
#[derive(Debug)]
pub struct FuseReader {
    /// The file descriptor to read from
    sender: ChannelSender,
}

impl FuseReader {
    /// Create a new reader from a channel sender.
    ///
    /// The sender's underlying fd is used for reading.
    pub fn new(sender: ChannelSender) -> Self {
        Self { sender }
    }

    /// Read a request from the kernel into the provided buffer.
    ///
    /// Returns the number of bytes read, or an error.
    pub fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        use std::os::unix::io::AsRawFd;

        let fd = self.sender.as_raw_fd();
        let rc = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };

        if rc < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(rc as usize)
        }
    }

    /// Create an owned request from the data in the buffer.
    #[inline]
    pub fn make_owned_request(&self, data: &[u8]) -> OwnedRequest {
        OwnedRequest::new(data, self.sender.clone())
    }

    /// Get a reference to the channel sender for replies.
    #[inline]
    pub fn sender(&self) -> &ChannelSender {
        &self.sender
    }
}

// FuseReader is Send because ChannelSender is Send
unsafe impl Send for FuseReader {}

// FuseReader is Sync because ChannelSender is Sync
unsafe impl Sync for FuseReader {}
