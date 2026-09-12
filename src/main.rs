//! TaskFlow — a minimal distributed job scheduler core.
//! Priority queue + worker pool + bounded retries, std only.

use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct Job {
    id: u64,
    topic: String,
    priority: u8,
    attempts: u8,
    max_attempts: u8,
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first, then lower id for FIFO within a priority.
        self.priority.cmp(&other.priority).then(other.id.cmp(&self.id))
    }
}
impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool { self.id == other.id }
}
impl Eq for Job {}

#[derive(Default)]
struct QueueState {
    heap: BinaryHeap<Job>,
    closed: bool,
    completed: u64,
    failed: u64,
}

struct Scheduler {
    state: Mutex<QueueState>,
    ready: Condvar,
}

impl Scheduler {
    fn new() -> Arc<Self> {
        Arc::new(Scheduler { state: Mutex::new(QueueState::default()), ready: Condvar::new() })
    }

    fn submit(&self, job: Job) {
        let mut st = self.state.lock().unwrap();
        st.heap.push(job);
        self.ready.notify_one();
    }

    /// Blocks until a job is available or the scheduler is drained.
    fn next(&self) -> Option<Job> {
        let mut st = self.state.lock().unwrap();
        loop {
            if let Some(job) = st.heap.pop() {
                return Some(job);
            }
            if st.closed {
                return None;
            }
            st = self.ready.wait(st).unwrap();
        }
    }

    fn close(&self) {
        self.state.lock().unwrap().closed = true;
        self.ready.notify_all();
    }
}

/// Stand-in for real work: fails deterministically on the first attempt of odd jobs.
fn run(job: &Job) -> Result<(), String> {
    thread::sleep(Duration::from_millis(5));
    if job.id % 2 == 1 && job.attempts == 0 {
        return Err(format!("transient error on {}", job.topic));
    }
    Ok(())
}

fn worker(id: usize, sched: Arc<Scheduler>) {
    while let Some(mut job) = sched.next() {
        match run(&job) {
            Ok(()) => {
                sched.state.lock().unwrap().completed += 1;
                println!("worker {id} ok    job={} topic={}", job.id, job.topic);
            }
            Err(e) => {
                job.attempts += 1;
                if job.attempts < job.max_attempts {
                    println!("worker {id} retry job={} ({e})", job.id);
                    sched.submit(job);
                } else {
                    sched.state.lock().unwrap().failed += 1;
                    println!("worker {id} drop  job={} ({e})", job.id);
                }
            }
        }
    }
}

fn main() {
    let sched = Scheduler::new();
    let topics = ["ingest", "index", "notify"];

    for id in 0..24u64 {
        sched.submit(Job {
            id,
            topic: topics[(id as usize) % topics.len()].to_string(),
            priority: (id % 3) as u8,
            attempts: 0,
            max_attempts: 3,
        });
    }

    let start = Instant::now();
    let workers: Vec<_> = (0..4)
        .map(|i| { let s = Arc::clone(&sched); thread::spawn(move || worker(i, s)) })
        .collect();

    thread::sleep(Duration::from_millis(300));
    sched.close();
    for w in workers { w.join().unwrap(); }

    let st = sched.state.lock().unwrap();
    println!("done: {} completed, {} failed in {:?}", st.completed, st.failed, start.elapsed());
}
