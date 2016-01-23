// The MIT License (MIT)

// Copyright (c) 2015 Rustcc Developers

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

//! Unix domain socket

use std::io::{self, Read, Write, ErrorKind};
use std::path::Path;
use std::ops::{Deref, DerefMut};
use std::convert::From;
use std::os::unix::io::{RawFd, AsRawFd, FromRawFd};
use std::cell::UnsafeCell;
use std::fmt;

use mio::{TryRead, TryWrite, TryAccept, EventSet, Evented, Timeout};

use scheduler::Scheduler;
use runtime::io::Io;

use super::IoTimeout;

pub struct UnixSocket {
    inner: ::mio::unix::UnixSocket,
    timeout: UnsafeCell<IoTimeout>,
}

impl fmt::Debug for UnixSocket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnixSocket {{ inner: {:?}, timeout: {:?} }}",
               self.inner,
               unsafe { &*self.timeout.get() })
    }
}

impl UnixSocket {
    fn new(inner: ::mio::unix::UnixSocket) -> UnixSocket {
        UnixSocket {
            inner: inner,
            timeout: UnsafeCell::new(IoTimeout::new()),
        }
    }

    /// Returns a new, unbound, non-blocking Unix domain socket
    pub fn stream() -> io::Result<UnixSocket> {
        ::mio::unix::UnixSocket::stream().map(UnixSocket::new)
    }

    /// Connect the socket to the specified address
    pub fn connect<P: AsRef<Path> + ?Sized>(self, addr: &P) -> io::Result<(UnixStream, bool)> {
        self.inner.connect(addr).map(|(s, completed)| (UnixStream::new(s), completed))
    }

    /// Bind the socket to the specified address
    pub fn bind<P: AsRef<Path> + ?Sized>(&self, addr: &P) -> io::Result<()> {
        self.inner.bind(addr)
    }

    /// Listen for incoming requests
    pub fn listen(self, backlog: usize) -> io::Result<UnixListener> {
        self.inner.listen(backlog).map(UnixListener::new)
    }

    pub fn try_clone(&self) -> io::Result<UnixSocket> {
        self.inner.try_clone().map(UnixSocket::new)
    }
}

impl Deref for UnixSocket {
    type Target = ::mio::unix::UnixSocket;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for UnixSocket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<::mio::unix::UnixSocket> for UnixSocket {
    fn from(sock: ::mio::unix::UnixSocket) -> UnixSocket {
        UnixSocket::new(sock)
    }
}

impl AsRawFd for UnixSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl FromRawFd for UnixSocket {
    unsafe fn from_raw_fd(fd: RawFd) -> UnixSocket {
        UnixSocket::new(FromRawFd::from_raw_fd(fd))
    }
}

impl Io for UnixSocket {
    fn evented(&self) -> &Evented {
        &self.inner
    }

    fn set_timeout(&self, timeout: Option<u64>) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.delay = timeout;
        }
    }

    fn timeout(&self) -> Option<u64> {
        unsafe {
            let to = &*self.timeout.get();
            to.delay.clone()
        }
    }

    fn save_timeout(&self, timeout: Timeout) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.timeout = Some(timeout);
        }
    }

    fn take_timeout(&self) -> Option<Timeout> {
        unsafe {
            let timeout = &mut *self.timeout.get();
            timeout.timeout.take()
        }
    }
}

pub struct UnixStream {
    inner: ::mio::unix::UnixStream,
    timeout: UnsafeCell<IoTimeout>,
}

impl fmt::Debug for UnixStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnixStream {{ inner: {:?}, timeout: {:?} }}",
               self.inner,
               unsafe { &*self.timeout.get() })
    }
}

impl UnixStream {
    fn new(inner: ::mio::unix::UnixStream) -> UnixStream {
        UnixStream {
            inner: inner,
            timeout: UnsafeCell::new(IoTimeout::new()),
        }
    }

    pub fn connect<P: AsRef<Path> + ?Sized>(path: &P) -> io::Result<UnixStream> {
        ::mio::unix::UnixStream::connect(path).map(UnixStream::new)
    }

    pub fn try_clone(&self) -> io::Result<UnixStream> {
        self.inner.try_clone().map(UnixStream::new)
    }
}

impl Read for UnixStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.inner.try_read(buf) {
            Ok(None) => {
                debug!("UnixStream read WouldBlock");
            }
            Ok(Some(len)) => {
                debug!("UnixStream read {} bytes", len);
                return Ok(len);
            }

            Err(err) => {
                return Err(err);
            }
        }

        loop {
            debug!("Read: Going to register event");
            try!(Scheduler::instance().unwrap().wait_event(self, EventSet::readable()));
            debug!("Read: Got read event");

            match self.inner.try_read(buf) {
                Ok(None) => {
                    debug!("UnixStream read WouldBlock");
                }
                Ok(Some(len)) => {
                    debug!("UnixStream read {} bytes", len);
                    return Ok(len);
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
    }
}

impl Write for UnixStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.inner.try_write(buf) {
            Ok(None) => {
                debug!("UnixStream write WouldBlock");
            }
            Ok(Some(len)) => {
                debug!("UnixStream written {} bytes", len);
                return Ok(len);
            }
            Err(err) => return Err(err),
        }

        loop {
            debug!("Write: Going to register event");
            try!(Scheduler::instance().unwrap().wait_event(self, EventSet::writable()));
            debug!("Write: Got write event");

            match self.inner.try_write(buf) {
                Ok(None) => {
                    debug!("UnixStream write WouldBlock");
                }
                Ok(Some(len)) => {
                    debug!("UnixStream written {} bytes", len);
                    return Ok(len);
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.inner.flush() {
            Ok(..) => return Ok(()),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                debug!("UnixStream flush WouldBlock");
            }
            Err(err) => return Err(err),
        }

        loop {
            debug!("Write: Going to register event");
            try!(Scheduler::instance().unwrap().wait_event(self, EventSet::writable()));
            debug!("Write: Got write event");

            match self.inner.flush() {
                Ok(..) => return Ok(()),
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    debug!("UnixStream flush WouldBlock");
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl Deref for UnixStream {
    type Target = ::mio::unix::UnixStream;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for UnixStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<::mio::unix::UnixStream> for UnixStream {
    fn from(sock: ::mio::unix::UnixStream) -> UnixStream {
        UnixStream::new(sock)
    }
}

impl AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl FromRawFd for UnixStream {
    unsafe fn from_raw_fd(fd: RawFd) -> UnixStream {
        UnixStream::new(FromRawFd::from_raw_fd(fd))
    }
}

impl Io for UnixStream {
    fn evented(&self) -> &Evented {
        &self.inner
    }

    fn set_timeout(&self, timeout: Option<u64>) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.delay = timeout;
        }
    }

    fn timeout(&self) -> Option<u64> {
        unsafe {
            let to = &*self.timeout.get();
            to.delay.clone()
        }
    }

    fn save_timeout(&self, timeout: Timeout) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.timeout = Some(timeout);
        }
    }

    fn take_timeout(&self) -> Option<Timeout> {
        unsafe {
            let timeout = &mut *self.timeout.get();
            timeout.timeout.take()
        }
    }
}

pub struct UnixListener {
    inner: ::mio::unix::UnixListener,
    timeout: UnsafeCell<IoTimeout>,
}

impl fmt::Debug for UnixListener {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnixListener {{ inner: {:?}, timeout: {:?} }}",
               self.inner,
               unsafe { &*self.timeout.get() })
    }
}

impl UnixListener {
    fn new(inner: ::mio::unix::UnixListener) -> UnixListener {
        UnixListener {
            inner: inner,
            timeout: UnsafeCell::new(IoTimeout::new()),
        }
    }

    pub fn bind<P: AsRef<Path> + ?Sized>(addr: &P) -> io::Result<UnixListener> {
        ::mio::unix::UnixListener::bind(addr).map(UnixListener::new)
    }

    pub fn accept(&self) -> io::Result<UnixStream> {
        match self.inner.accept() {
            Ok(None) => {
                debug!("UnixListener accept WouldBlock; going to register into eventloop");
            }
            Ok(Some(stream)) => {
                return Ok(UnixStream::new(stream));
            }
            Err(err) => {
                return Err(err);
            }
        }

        loop {
            try!(Scheduler::instance().unwrap().wait_event(self, EventSet::readable()));

            match self.inner.accept() {
                Ok(None) => {
                    warn!("UnixListener accept WouldBlock; Coroutine was awaked by readable event");
                }
                Ok(Some(stream)) => {
                    return Ok(UnixStream::new(stream));
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
    }

    pub fn try_clone(&self) -> io::Result<UnixListener> {
        self.inner.try_clone().map(UnixListener::new)
    }
}

impl Deref for UnixListener {
    type Target = ::mio::unix::UnixListener;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for UnixListener {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Io for UnixListener {
    fn evented(&self) -> &Evented {
        &self.inner
    }

    fn set_timeout(&self, timeout: Option<u64>) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.delay = timeout;
        }
    }

    fn timeout(&self) -> Option<u64> {
        unsafe {
            let to = &*self.timeout.get();
            to.delay.clone()
        }
    }

    fn save_timeout(&self, timeout: Timeout) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.timeout = Some(timeout);
        }
    }

    fn take_timeout(&self) -> Option<Timeout> {
        unsafe {
            let timeout = &mut *self.timeout.get();
            timeout.timeout.take()
        }
    }
}

impl From<::mio::unix::UnixListener> for UnixListener {
    fn from(listener: ::mio::unix::UnixListener) -> UnixListener {
        UnixListener::new(listener)
    }
}

impl AsRawFd for UnixListener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl FromRawFd for UnixListener {
    unsafe fn from_raw_fd(fd: RawFd) -> UnixListener {
        UnixListener::new(FromRawFd::from_raw_fd(fd))
    }
}

pub fn pipe() -> io::Result<(PipeReader, PipeWriter)> {
    ::mio::unix::pipe().map(|(r, w)| (PipeReader::new(r), PipeWriter::new(w)))
}

pub struct PipeReader {
    inner: ::mio::unix::PipeReader,
    timeout: UnsafeCell<IoTimeout>,
}

impl fmt::Debug for PipeReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PipeReader {{ inner: {:?}, timeout: {:?} }}",
               self.inner,
               unsafe { &*self.timeout.get() })
    }
}

impl PipeReader {
    fn new(inner: ::mio::unix::PipeReader) -> PipeReader {
        PipeReader {
            inner: inner,
            timeout: UnsafeCell::new(IoTimeout::new()),
        }
    }
}

impl Read for PipeReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.inner.try_read(buf) {
            Ok(None) => {
                debug!("PipeReader read WouldBlock");
            }
            Ok(Some(len)) => {
                debug!("PipeReader read {} bytes", len);
                return Ok(len);
            }

            Err(err) => {
                return Err(err);
            }
        }

        loop {
            debug!("Read: Going to register event");
            try!(Scheduler::instance().unwrap().wait_event(self, EventSet::readable()));
            debug!("Read: Got read event");

            match self.inner.try_read(buf) {
                Ok(None) => {
                    debug!("PipeReader read WouldBlock");
                }
                Ok(Some(len)) => {
                    debug!("PipeReader read {} bytes", len);
                    return Ok(len);
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
    }
}

impl Deref for PipeReader {
    type Target = ::mio::unix::PipeReader;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PipeReader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Io for PipeReader {
    fn evented(&self) -> &Evented {
        &self.inner
    }

    fn set_timeout(&self, timeout: Option<u64>) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.delay = timeout;
        }
    }

    fn timeout(&self) -> Option<u64> {
        unsafe {
            let to = &*self.timeout.get();
            to.delay.clone()
        }
    }

    fn save_timeout(&self, timeout: Timeout) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.timeout = Some(timeout);
        }
    }

    fn take_timeout(&self) -> Option<Timeout> {
        unsafe {
            let timeout = &mut *self.timeout.get();
            timeout.timeout.take()
        }
    }
}

impl From<::mio::unix::PipeReader> for PipeReader {
    fn from(listener: ::mio::unix::PipeReader) -> PipeReader {
        PipeReader::new(listener)
    }
}

impl AsRawFd for PipeReader {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl FromRawFd for PipeReader {
    unsafe fn from_raw_fd(fd: RawFd) -> PipeReader {
        PipeReader::new(FromRawFd::from_raw_fd(fd))
    }
}

pub struct PipeWriter {
    inner: ::mio::unix::PipeWriter,
    timeout: UnsafeCell<IoTimeout>,
}

impl fmt::Debug for PipeWriter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PipeWriter {{ inner: {:?}, timeout: {:?} }}",
               self.inner,
               unsafe { &*self.timeout.get() })
    }
}

impl PipeWriter {
    fn new(inner: ::mio::unix::PipeWriter) -> PipeWriter {
        PipeWriter {
            inner: inner,
            timeout: UnsafeCell::new(IoTimeout::new()),
        }
    }
}

impl Write for PipeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.inner.try_write(buf) {
            Ok(None) => {
                debug!("PipeWriter write WouldBlock");
            }
            Ok(Some(len)) => {
                debug!("PipeWriter written {} bytes", len);
                return Ok(len);
            }
            Err(err) => return Err(err),
        }

        loop {
            debug!("Write: Going to register event");
            try!(Scheduler::instance().unwrap().wait_event(self, EventSet::writable()));
            debug!("Write: Got write event");

            match self.inner.try_write(buf) {
                Ok(None) => {
                    debug!("PipeWriter write WouldBlock");
                }
                Ok(Some(len)) => {
                    debug!("PipeWriter written {} bytes", len);
                    return Ok(len);
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.inner.flush() {
            Ok(..) => return Ok(()),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                debug!("PipeWriter flush WouldBlock");
            }
            Err(err) => return Err(err),
        }

        loop {
            debug!("Write: Going to register event");
            try!(Scheduler::instance().unwrap().wait_event(self, EventSet::writable()));
            debug!("Write: Got write event");

            match self.inner.flush() {
                Ok(..) => return Ok(()),
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    debug!("PipeWriter flush WouldBlock");
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl Deref for PipeWriter {
    type Target = ::mio::unix::PipeWriter;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PipeWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Io for PipeWriter {
    fn evented(&self) -> &Evented {
        &self.inner
    }

    fn set_timeout(&self, timeout: Option<u64>) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.delay = timeout;
        }
    }

    fn timeout(&self) -> Option<u64> {
        unsafe {
            let to = &*self.timeout.get();
            to.delay.clone()
        }
    }

    fn save_timeout(&self, timeout: Timeout) {
        unsafe {
            let to = &mut *self.timeout.get();
            to.timeout = Some(timeout);
        }
    }

    fn take_timeout(&self) -> Option<Timeout> {
        unsafe {
            let timeout = &mut *self.timeout.get();
            timeout.timeout.take()
        }
    }
}

impl From<::mio::unix::PipeWriter> for PipeWriter {
    fn from(listener: ::mio::unix::PipeWriter) -> PipeWriter {
        PipeWriter::new(listener)
    }
}

impl AsRawFd for PipeWriter {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl FromRawFd for PipeWriter {
    unsafe fn from_raw_fd(fd: RawFd) -> PipeWriter {
        PipeWriter::new(FromRawFd::from_raw_fd(fd))
    }
}
