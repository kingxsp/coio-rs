extern crate coio;
extern crate time;

use time::PreciseTime;

use coio::{spawn, Scheduler};
use coio::sync::mpsc::{channel, Sender};

fn create_node(next: Sender<usize>) -> Sender<usize> {
    let (send, recv) = channel::<usize>();
    spawn(move || {
        loop {
            let i = recv.recv().unwrap();
            if i == 0 { break; }
            next.send(i + 1).unwrap();
        }
        next.send(0).unwrap();
    });
    send
}

fn master(iters: usize, size: usize) {
    let t0 = PreciseTime::now();
    let (mut send, recv) = channel::<usize>();
    for _ in 0..size-1 {
        send = create_node(send);
    }
    let t1 = PreciseTime::now();
    println!("Ring Created");
    let mut i = 0;
    for _ in 0..iters {
        send.send(i + 1).unwrap();
        i = recv.recv().unwrap();
    }
    let t2 = PreciseTime::now();
    println!("{}", i);
    send.send(0).unwrap();
    recv.recv().unwrap();
    println!("Creation time: {}", t0.to(t1));
    println!("Messaging time: {}", t1.to(t2));
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let iters = args[1].parse().unwrap();
    let size = args[2].parse().unwrap();
    let procs = args[3].parse().unwrap();
    let _ = Scheduler::new().with_workers(procs).run(move || {
        master(iters, size);
    });
}
