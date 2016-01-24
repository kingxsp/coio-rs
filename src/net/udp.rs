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

//! UDP

use std::ops::{Deref, DerefMut};
use std::io;
use std::net::{ToSocketAddrs, SocketAddr};
use std::cell::UnsafeCell;
use std::fmt;

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

use mio::{EventSet, Evented, Timeout};

use scheduler::Scheduler;
use io::Io;

use super::IoTimeout;

pub struct UdpSocket {
    inner: ::mio::udp::UdpSocket,
    timeout: UnsafeCell<IoTimeout>,
}

impl fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UdpSocket {{ inner: {:?}, timeout: {:?} }}",
               self.inner,
               unsafe { &*self.timeout.get() })
    }
}

impl UdpSocket {
    fn new(inner: ::mio::udp::UdpSocket) -> UdpSocket {
        UdpSocket {
            inner: inner,
            timeout: UnsafeCell::new(IoTimeout::new()),
        }
    }

    /// Returns a new, unbound, non-blocking, IPv4 UDP socket
    pub fn v4() -> io::Result<UdpSocket> {
        Ok(UdpSocket::new(try!(::mio::udp::UdpSocket::v4())))
    }

    /// Returns a new, unbound, non-blocking, IPv6 UDP socket
    pub fn v6() -> io::Result<UdpSocket> {
        Ok(UdpSocket::new(try!(::mio::udp::UdpSocket::v6())))
    }

    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<UdpSocket> {
        super::each_addr(addr, |a| ::mio::udp::UdpSocket::bound(&a)).map(UdpSocket::new)
    }

    pub fn try_clone(&self) -> io::Result<UdpSocket> {
        Ok(UdpSocket::new(try!(self.inner.try_clone())))
    }

    pub fn send_to<A: ToSocketAddrs>(&self, buf: &[u8], target: A) -> io::Result<usize> {
        let mut last_err = Ok(0);
        for addr in try!(target.to_socket_addrs()) {
            match self.inner.send_to(buf, &addr) {
                Ok(None) => {
                    debug!("UdpSocket send_to WOULDBLOCK");

                    loop {
                        try!(Scheduler::instance()
                                 .unwrap()
                                 .wait_event(self, EventSet::writable()));

                        match self.inner.send_to(buf, &addr) {
                            Ok(None) => {
                                warn!("UdpSocket send_to WOULDBLOCK");
                            }
                            Ok(Some(len)) => {
                                return Ok(len);
                            }
                            Err(err) => {
                                return Err(err);
                            }
                        }
                    }
                }
                Ok(Some(len)) => {
                    return Ok(len);
                }
                Err(err) => last_err = Err(err),
            }
        }

        last_err
    }

    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        match try!(self.inner.recv_from(buf)) {
            None => {
                debug!("UdpSocket recv_from WOULDBLOCK");
            }
            Some(ret) => {
                return Ok(ret);
            }
        }

        loop {
            try!(Scheduler::instance().unwrap().wait_event(self, EventSet::readable()));

            match try!(self.inner.recv_from(buf)) {
                None => {
                    warn!("UdpSocket recv_from WOULDBLOCK");
                }
                Some(ret) => {
                    return Ok(ret);
                }
            }
        }
    }
}

impl Deref for UdpSocket {
    type Target = ::mio::udp::UdpSocket;

    fn deref(&self) -> &::mio::udp::UdpSocket {
        &self.inner
    }
}

impl DerefMut for UdpSocket {
    fn deref_mut(&mut self) -> &mut ::mio::udp::UdpSocket {
        &mut self.inner
    }
}

#[cfg(unix)]
impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(unix)]
impl FromRawFd for UdpSocket {
    unsafe fn from_raw_fd(fd: RawFd) -> UdpSocket {
        UdpSocket::new(FromRawFd::from_raw_fd(fd))
    }
}

impl Io for UdpSocket {
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
