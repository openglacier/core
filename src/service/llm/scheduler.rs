#![cfg_attr(rustfmt, rustfmt_skip)]
//! Bounded FIFO scheduling and cancellation for local LLM generations.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
};

use super::runtime::LlmError;

#[derive(Debug, Clone)]
pub struct LlmScheduler {
    inner: Arc<SchedulerInner>,
}

#[derive(Debug)]
struct SchedulerInner {
    max_parallel: usize,
    queue_size: usize,
    next_run_id: AtomicU64,
    state: Mutex<SchedulerState>,
    wake: Condvar,
}

#[derive(Debug, Default)]
struct SchedulerState {
    active: usize,
    queued: VecDeque<u64>,
    runs: HashMap<u64, RunControl>,
}

impl LlmScheduler {
    #[must_use]
    pub fn new(max_parallel: u32, queue_size: u32) -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                max_parallel: max_parallel as usize,
                queue_size: queue_size as usize,
                next_run_id: AtomicU64::new(1),
                state: Mutex::new(SchedulerState::default()),
                wake: Condvar::new(),
            }),
        }
    }

    pub fn schedule(&self, owner: String) -> Result<ScheduledRun, LlmError> {
        let run_id = self.inner.next_run_id.fetch_add(1, Ordering::Relaxed);
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| LlmError::Runtime("LLM scheduler lock poisoned".to_owned()))?;

        let active = if state.active < self.inner.max_parallel && state.queued.is_empty() {
            state.active += 1;
            true
        } else {
            if state.queued.len() >= self.inner.queue_size {
                return Err(LlmError::Busy(format!(
                    "LLM queue is full (maxParallel={}, queueSize={})",
                    self.inner.max_parallel, self.inner.queue_size
                )));
            }
            state.queued.push_back(run_id);
            false
        };
        state.runs.insert(
            run_id,
            RunControl {
                owner,
                cancelled: Arc::clone(&cancelled),
            },
        );
        drop(state);

        Ok(ScheduledRun {
            inner: Arc::clone(&self.inner),
            run_id,
            cancelled,
            active,
            queued: !active,
        })
    }

    #[must_use]
    pub fn cancel(&self, run_id: u64, owner: &str) -> bool {
        let cancelled = self.inner.state.lock().ok().and_then(|state| {
            state
                .runs
                .get(&run_id)
                .filter(|run| run.owner == owner)
                .map(|run| Arc::clone(&run.cancelled))
        });
        if let Some(cancelled) = cancelled {
            cancelled.store(true, Ordering::Relaxed);
            self.inner.wake.notify_all();
            true
        } else {
            false
        }
    }

    #[must_use]
    pub fn active_requests(&self) -> usize {
        self.inner.state.lock().map_or(0, |state| state.active)
    }

    #[must_use]
    pub fn queued_requests(&self) -> usize {
        self.inner
            .state
            .lock()
            .map_or(0, |state| state.queued.len())
    }

    #[must_use]
    pub fn max_parallel(&self) -> u32 {
        self.inner.max_parallel as u32
    }

    #[must_use]
    pub fn queue_size(&self) -> u32 {
        self.inner.queue_size as u32
    }
}

#[derive(Debug)]
struct RunControl {
    owner: String,
    cancelled: Arc<AtomicBool>,
}

#[derive(Debug)]
pub struct ScheduledRun {
    inner: Arc<SchedulerInner>,
    run_id: u64,
    cancelled: Arc<AtomicBool>,
    active: bool,
    queued: bool,
}

impl ScheduledRun {
    #[must_use]
    pub const fn run_id(&self) -> u64 {
        self.run_id
    }

    #[must_use]
    pub const fn initial_state(&self) -> &'static str {
        if self.active {
            "running"
        } else {
            "queued"
        }
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Wait until this run owns one execution slot.
    ///
    /// Returns `false` if the run was cancelled while queued.
    pub fn wait_until_active(&mut self) -> Result<bool, LlmError> {
        if self.active {
            return Ok(!self.is_cancelled());
        }

        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| LlmError::Runtime("LLM scheduler lock poisoned".to_owned()))?;
        loop {
            if self.is_cancelled() {
                if self.queued {
                    remove_queued(&mut state.queued, self.run_id);
                    self.queued = false;
                }
                self.inner.wake.notify_all();
                return Ok(false);
            }

            let is_front = state.queued.front().copied() == Some(self.run_id);
            if is_front && state.active < self.inner.max_parallel {
                let popped = state.queued.pop_front();
                debug_assert_eq!(popped, Some(self.run_id));
                state.active += 1;
                self.active = true;
                self.queued = false;
                self.inner.wake.notify_all();
                return Ok(true);
            }

            state = self
                .inner
                .wake
                .wait(state)
                .map_err(|_| LlmError::Runtime("LLM scheduler lock poisoned".to_owned()))?;
        }
    }
}

impl Drop for ScheduledRun {
    fn drop(&mut self) {
        let Ok(mut state) = self.inner.state.lock() else {
            return;
        };
        if self.active {
            state.active = state.active.saturating_sub(1);
            self.active = false;
        }
        if self.queued {
            remove_queued(&mut state.queued, self.run_id);
            self.queued = false;
        }
        state.runs.remove(&self.run_id);
        self.inner.wake.notify_all();
    }
}

fn remove_queued(queue: &mut VecDeque<u64>, run_id: u64) {
    if let Some(index) = queue.iter().position(|candidate| *candidate == run_id) {
        queue.remove(index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn queue_is_bounded() { let scheduler = LlmScheduler::new(1, 1); let _active = scheduler.schedule("a".to_owned()).unwrap(); let _queued = scheduler.schedule("a".to_owned()).unwrap(); assert!(matches!( scheduler.schedule("a".to_owned()), Err(LlmError::Busy(_)) )); }
    #[test] fn cancelling_unknown_run_is_false() { let scheduler = LlmScheduler::new(1, 1); assert!(!scheduler.cancel(42, "a")); }
    #[test] fn cancellation_is_scoped_to_owner() { let scheduler = LlmScheduler::new(1, 1); let run = scheduler.schedule("owner-a".to_owned()).unwrap(); assert!(!scheduler.cancel(run.run_id(), "owner-b")); assert!(scheduler.cancel(run.run_id(), "owner-a")); }
}
