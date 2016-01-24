use mio::{Evented, Timeout};

pub trait Io {
    fn evented(&self) -> &Evented;

    fn set_timeout(&self, timeout: Option<u64>);
    fn timeout(&self) -> Option<u64>;

    fn save_timeout(&self, timeout: Timeout);
    fn take_timeout(&self) -> Option<Timeout>;
}
