// The MIT License (MIT)

// Copyright (c) 2015 Y. T. Chung <zonyitoo@gmail.com>

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

//! Processing unit of a thread

use rand::Rng;
use std::any::Any;
use std::boxed::FnBox;
use std::cell::UnsafeCell;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Weak};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, Builder};

use deque::{BufferPool, Stolen, Worker, Stealer};
use rand;

use coroutine::{Coroutine, State, Handle};
use options::Options;
use scheduler::Scheduler;

thread_local!(static PROCESSOR: UnsafeCell<Option<Processor>> = UnsafeCell::new(None));

#[derive(Debug)]
pub struct ForceUnwind;

#[derive(Clone)]
pub struct Processor {
    inner: Arc<ProcessorInner>,
}

unsafe impl Send for Processor {}
unsafe impl Sync for Processor {}

/// Processing unit of a thread
pub struct ProcessorInner {
    weak_self: WeakProcessor,
    scheduler: *mut Scheduler,

    // Stores the context of the Processor::schedule() loop.
    main_coro: Handle,

    // NOTE: ONLY to be used by resume() and take_current_coroutine().
    current_coro: Option<Handle>,

    // NOTE: ONLY to be used to communicate the result from yield_with() to resume().
    last_state: State,

    rng: rand::XorShiftRng,
    queue_worker: Worker<Handle>,
    queue_stealer: Stealer<Handle>,
    neighbor_stealers: Vec<Stealer<Handle>>, // TODO: make it a Arc<Vec<>>
    take_coro_cb: Option<&'static mut FnMut(Handle)>,

    chan_sender: Sender<ProcMessage>,
    chan_receiver: Receiver<ProcMessage>,

    is_exiting: bool,
}

impl Processor {
    fn new_with_neighbors(sched: *mut Scheduler, neigh: Vec<Stealer<Handle>>) -> Processor {
        let (worker, stealer) = BufferPool::new().deque();
        let (tx, rx) = mpsc::channel();

        let mut p = Processor {
            inner: Arc::new(ProcessorInner {
                weak_self: unsafe { mem::zeroed() },
                scheduler: sched,

                main_coro: unsafe { Coroutine::empty() },
                current_coro: None,
                last_state: State::Suspended,

                rng: rand::weak_rng(),
                queue_worker: worker,
                queue_stealer: stealer,
                neighbor_stealers: neigh,
                take_coro_cb: None,

                chan_sender: tx,
                chan_receiver: rx,

                is_exiting: false,
            }),
        };

        {
            let weak_self = WeakProcessor { inner: Arc::downgrade(&p.inner) };
            let inner = p.deref_mut();
            mem::forget(mem::replace(&mut inner.weak_self, weak_self));
        }

        p
    }

    fn set_tls(p: &Processor) {
        PROCESSOR.with(|proc_opt| unsafe {
            // HACK: Wohooo!
            let proc_opt = &mut *proc_opt.get();
            *proc_opt = Some(p.clone());
        })
    }

    pub fn run_with_neighbors(processor_id: usize,
                              sched: *mut Scheduler,
                              neigh: Vec<Stealer<Handle>>)
                              -> (thread::JoinHandle<()>, Sender<ProcMessage>, Stealer<Handle>) {
        let mut p = Processor::new_with_neighbors(sched, neigh);
        let msg = p.handle();
        let st = p.stealer();

        let hdl = Builder::new()
                      .name(format!("Processor #{}", processor_id))
                      .spawn(move || {
                          Processor::set_tls(&mut p);
                          p.schedule();
                      })
                      .unwrap();

        (hdl, msg, st)
    }

    pub fn run_main<M, T>(processor_id: usize,
                          sched: *mut Scheduler,
                          f: M)
                          -> (thread::JoinHandle<()>,
                              Sender<ProcMessage>,
                              Stealer<Handle>,
                              ::std::sync::mpsc::Receiver<Result<T, Box<Any + Send + 'static>>>)
        where M: FnOnce() -> T + Send + 'static,
              T: Send + 'static
    {
        let mut p = Processor::new_with_neighbors(sched, Vec::new());
        let (msg, st) = (p.handle(), p.stealer());
        let (tx, rx) = ::std::sync::mpsc::channel();

        let hdl =
            Builder::new()
                .name(format!("Processor #{}", processor_id))
                .spawn(move || {
                    Processor::set_tls(&mut p);

                    let wrapper = move || {
                        let ret = unsafe { ::try(move || f()) };

                        // If sending fails Scheduler::run()'s loop would never quit --> unwrap.
                        tx.send(ret).unwrap();
                    };
                    p.spawn_opts(Box::new(wrapper), Options::default());

                    p.schedule();
                })
                .unwrap();

        (hdl, msg, st, rx)
    }

    pub fn scheduler(&self) -> &Scheduler {
        unsafe { &*self.scheduler }
    }

    pub unsafe fn mut_ptr(&self) -> *mut Processor {
        mem::transmute(self)
    }

    /// Get the thread local processor. NOT thread safe!
    pub fn current() -> Option<Processor> {
        PROCESSOR.with(|proc_opt| unsafe { (&*proc_opt.get()).clone() })
    }

    /// Obtains the currently running coroutine after setting it's state to Blocked.
    /// NOTE: DO NOT call any Scheduler or Processor method within the passed callback, other than ready().
    pub fn take_current_coroutine<U, F>(&mut self, f: F) -> U
        where F: FnOnce(Handle) -> U
    {
        let mut f = Some(f);
        let mut r = None;

        {
            let mut cb = |coro: Handle| r = Some(f.take().unwrap()(coro));

            // NOTE: Circumvents the following problem:
            //   transmute called with differently sized types: &mut [closure ...] (could be 64 bits) to
            //   &'static mut core::ops::FnMut(Box<coroutine::Coroutine>) + 'static (could be 128 bits) [E0512]
            let cb_ref: &mut FnMut(Handle) = &mut cb;
            let cb_ref_static = unsafe { mem::transmute(cb_ref) };

            // Gets executed as soon as yield_with() returns in Processor::resume().
            self.take_coro_cb = Some(cb_ref_static);
            self.yield_with(State::Blocked);
        }

        r.unwrap()
    }

    pub fn stealer(&self) -> Stealer<Handle> {
        self.queue_stealer.clone()
    }

    pub fn handle(&self) -> Sender<ProcMessage> {
        self.chan_sender.clone()
    }

    pub fn spawn_opts(&mut self, f: Box<FnBox()>, opts: Options) {
        let mut new_coro = Coroutine::spawn_opts(f, opts);
        new_coro.set_preferred_processor(Some(self.weak_self.clone()));

        // NOTE: If Scheduler::spawn() is called we want to make
        // sure that the spawned coroutine is executed immediately.
        // TODO: Should we really do this?
        if self.current_coro.is_some() {
            // Circumvent borrowck
            let queue_worker = &self.queue_worker as *const Worker<Handle>;

            self.take_current_coroutine(|coro| unsafe {
                // queue_worker.push() inserts at the front of the queue.
                // --> Insert new_coro last to ensure that it's at the front of the queue.
                (&*queue_worker).push(coro);
                (&*queue_worker).push(new_coro);
            });
        } else {
            self.ready(new_coro);
        }
    }

    /// Run the processor
    fn schedule(&mut self) {
        'outerloop: loop {
            // 1. Run all tasks in local queue
            while let Some(hdl) = self.queue_worker.pop() {
                self.resume(hdl);
            }

            // NOTE: It's important that this block comes right after the loop above.
            // The chan_receiver loop below is the only place a Shutdown message can be received.
            // Right after receiving one it will continue the 'outerloop from the beginning,
            // resume() all coroutines in the queue_worker which will ForceUnwind
            // and after that we exit the 'outerloop here.
            if self.is_exiting {
                break;
            }

            // 2. Check the mainbox
            {
                let mut resume_all_tasks = false;

                while let Ok(msg) = self.chan_receiver.try_recv() {
                    match msg {
                        ProcMessage::NewNeighbor(nei) => self.neighbor_stealers.push(nei),
                        ProcMessage::Shutdown => {
                            self.is_exiting = true;
                            resume_all_tasks = true;
                        }
                        ProcMessage::Ready(mut coro) => {
                            coro.set_preferred_processor(Some(self.weak_self.clone()));
                            self.ready(coro);
                            resume_all_tasks = true;
                        }
                    }
                }

                // Prefer running own tasks before stealing --> "continue" from anew.
                if resume_all_tasks {
                    continue;
                }
            }

            // 3. Randomly steal from neighbors as a last measure.
            // TODO: To improve cache locality foreign lists should be split in half or so instead.
            let rand_idx = self.rng.gen::<usize>();
            let total_stealers = self.neighbor_stealers.len();

            for idx in 0..total_stealers {
                let idx = (rand_idx + idx) % total_stealers;

                if let Stolen::Data(hdl) = self.neighbor_stealers[idx].steal() {
                    self.resume(hdl);
                    continue 'outerloop;
                }
            }

            // Wait forever until we got notified
            // TODO:
            //   Could this be improved somehow?
            //   Maybe by implementing a "processor-pool" akin to a thread-pool,
            //   which would move park()ed Processors to a shared idle-queue.
            //   Other Processors could then unpark() them as necessary in their own ready() method.
            if let Ok(msg) = self.chan_receiver.recv() {
                match msg {
                    ProcMessage::NewNeighbor(nei) => self.neighbor_stealers.push(nei),
                    ProcMessage::Shutdown => {
                        self.is_exiting = true;
                        continue 'outerloop;
                    }
                    ProcMessage::Ready(mut coro) => {
                        coro.set_preferred_processor(Some(self.weak_self.clone()));
                        self.ready(coro);
                    }
                }
            };
        }
    }

    fn resume(&mut self, coro: Handle) {
        unsafe {
            let current_coro: *const Coroutine = &*coro;
            
            self.current_coro = Some(coro);
            self.main_coro.yield_to(&*current_coro);
        }

        let coro = self.current_coro.take().unwrap();

        match self.last_state {
            State::Suspended => {
                self.ready(coro);
            }
            State::Blocked => {
                self.take_coro_cb.take().unwrap()(coro);
            }
            State::Finished => {
                Scheduler::finished(coro);
            }
        }
    }

    /// Enqueue a coroutine to be resumed as soon as possible (making it the head of the queue)
    pub fn ready(&mut self, coro: Handle) {
        self.queue_worker.push(coro);
    }

    /// Suspends the current running coroutine, equivalent to `Scheduler::sched`
    pub fn sched(&mut self) {
        self.yield_with(State::Suspended)
    }

    /// Yield the current running coroutine with specified result
    pub fn yield_with(&mut self, r: State) {
        self.last_state = r;

        unsafe {
            let main_coro: *const Coroutine = &*self.main_coro;
            self.current_coro.as_mut().unwrap().yield_to(&*main_coro);
        }

        // We are back! Exit right now!
        if self.is_exiting {
            panic!(ForceUnwind);
        }
    }
}

impl Deref for Processor {
    type Target = ProcessorInner;

    #[inline]
    fn deref(&self) -> &ProcessorInner {
        self.inner.deref()
    }
}

impl DerefMut for Processor {
    #[inline]
    fn deref_mut(&mut self) -> &mut ProcessorInner {
        unsafe { &mut *(self.inner.deref() as *const ProcessorInner as *mut ProcessorInner) }
    }
}

impl PartialEq for Processor {
    fn eq(&self, other: &Processor) -> bool {
        (self as *const Processor) == (other as *const Processor)
    }
}

impl Eq for Processor {}

// For coroutine.rs
#[derive(Clone)]
pub struct WeakProcessor {
    inner: Weak<ProcessorInner>,
}

unsafe impl Send for WeakProcessor {}
unsafe impl Sync for WeakProcessor {}

impl WeakProcessor {
    pub fn upgrade(&self) -> Option<Processor> {
        self.inner.upgrade().and_then(|p| Some(Processor { inner: p }))
    }
}

pub enum ProcMessage {
    NewNeighbor(Stealer<Handle>),
    Ready(Handle),
    Shutdown,
}
