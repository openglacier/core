//! Connection admission bounded by the memory governor.
//!
//! Every served connection costs at least one OS thread plus its buffers.
//! Instead of a dedicated connection limit, each connection reserves an
//! estimated footprint in the governor's `Network` class: capacity follows the
//! configured memory profile, and page-cache reclaim and process pressure
//! apply to connections like to any other governed allocation.
//!
//! Connections that cannot be admitted wait in a FIFO queue in which local
//! connections (the admin CLI) go before Gateway channels, so an operator can
//! still reach a node that is being flooded through its Gateway.
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex, PoisonError},
    time::{Duration, Instant},
};

use crate::{
    helpers::lock_unpoisoned,
    memory::{MemoryClass, MemoryGovernor, MemoryProfileConfig, MemoryReservation},
};

/// Estimated resident cost of one connection thread: committed stack, the
/// buffered reader, the retained request buffer and bridge buffers.
pub(super) const CONNECTION_THREAD_FOOTPRINT_BYTES: usize = 256 * 1024;

/// Memory released by queries or caches does not signal the queue, so the
/// head waiter re-evaluates capacity at this interval.
const ADMISSION_RECHECK_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AdmissionPriority {
    Local,
    Gateway,
}

#[derive(Debug)]
pub(super) struct ConnectionAdmission {
    governor: MemoryGovernor,
    budget_bytes: usize,
    state: Mutex<AdmissionState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct AdmissionState {
    next_ticket: u64,
    waiting: VecDeque<(u64, AdmissionPriority)>,
    active_bytes: usize,
    /// Governed bytes observed when the governor last refused the head
    /// waiter. Retrying before that total shrinks would only fill the
    /// governor event journal with rejections.
    refused_at_reserved_bytes: Option<usize>,
}

impl AdmissionState {
    fn head(&self) -> Option<u64> {
        self.waiting
            .iter()
            .find(|(_, priority)| *priority == AdmissionPriority::Local)
            .or_else(|| self.waiting.front())
            .map(|(ticket, _)| *ticket)
    }

    fn leave(&mut self, ticket: u64) {
        self.waiting.retain(|(waiting, _)| *waiting != ticket);
    }
}

/// Admitted connection. Dropping it returns the footprint to the governor and
/// wakes the queue.
#[derive(Debug)]
pub(super) struct ConnectionPermit {
    admission: Arc<ConnectionAdmission>,
    bytes: usize,
    reservation: Option<MemoryReservation>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        drop(self.reservation.take());
        let mut state = lock_unpoisoned(&self.admission.state);
        state.active_bytes = state.active_bytes.saturating_sub(self.bytes);
        state.refused_at_reserved_bytes = None;
        drop(state);
        self.admission.changed.notify_all();
    }
}

/// Share of governed memory that connections may hold.
fn connection_budget_bytes(profile: MemoryProfileConfig) -> usize {
    match profile.managed_budget_bytes {
        // Connections share the managed budget with caches and the planner,
        // never with the operation envelope reserved for queries and imports.
        Some(managed) => managed.saturating_sub(profile.operation_budget_bytes),
        // Without a process limit the profile is sized from host memory.
        None => profile.operation_budget_bytes,
    }
}

impl ConnectionAdmission {
    pub(super) fn new(governor: MemoryGovernor) -> Self {
        Self {
            budget_bytes: connection_budget_bytes(governor.profile()),
            governor,
            state: Mutex::new(AdmissionState::default()),
            changed: Condvar::new(),
        }
    }

    /// Waits as long as needed for capacity.
    pub(super) fn acquire(self: &Arc<Self>, threads: usize, priority: AdmissionPriority) -> ConnectionPermit {
        self.acquire_until(threads, priority, None)
            .expect("admission without deadline always succeeds")
    }

    /// Waits for capacity until `deadline`, or indefinitely without one.
    pub(super) fn acquire_until( self: &Arc<Self>, threads: usize, priority: AdmissionPriority, deadline: Option<Instant>, ) -> Option<ConnectionPermit> {
        let bytes = CONNECTION_THREAD_FOOTPRINT_BYTES.saturating_mul(threads);
        let mut state = lock_unpoisoned(&self.state);
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        state.waiting.push_back((ticket, priority));
        loop {
            if state.head() == Some(ticket) {
                if let Some(reservation) = self.try_reserve(&mut state, bytes) {
                    state.leave(ticket);
                    state.active_bytes = state.active_bytes.saturating_add(bytes);
                    drop(state);
                    self.changed.notify_all();
                    return Some(ConnectionPermit {
                        admission: Arc::clone(self),
                        bytes,
                        reservation: Some(reservation),
                    });
                }
            }
            let mut wait = ADMISSION_RECHECK_INTERVAL;
            if let Some(deadline) = deadline {
                let now = Instant::now();
                if now >= deadline {
                    state.leave(ticket);
                    drop(state);
                    self.changed.notify_all();
                    return None;
                }
                wait = wait.min(deadline - now);
            }
            state = self
                .changed
                .wait_timeout(state, wait)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    fn try_reserve(&self, state: &mut AdmissionState, bytes: usize) -> Option<MemoryReservation> {
        // An idle node always admits one connection, whatever the profile.
        if state.active_bytes > 0 && state.active_bytes.saturating_add(bytes) > self.budget_bytes {
            return None;
        }
        if state
            .refused_at_reserved_bytes
            .is_some_and(|refused| self.governor.reserved_bytes() >= refused)
        {
            return None;
        }
        self.governor.ensure_process_capacity(MemoryClass::Network, bytes).ok()?;
        match self.governor.reserve(MemoryClass::Network, bytes) {
            Ok(reservation) => {
                state.refused_at_reserved_bytes = None;
                Some(reservation)
            }
            Err(_) => {
                state.refused_at_reserved_bytes = Some(self.governor.reserved_bytes());
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    const MIB: usize = 1024 * 1024;

    fn admission_with_limit(limit_bytes: usize) -> Arc<ConnectionAdmission> {
        Arc::new(ConnectionAdmission::new(MemoryGovernor::with_process_limit(limit_bytes)))
    }

    #[test]
    fn budget_excludes_the_operation_envelope() {
        let profile = MemoryProfileConfig::for_limit(256 * MIB);
        let budget = connection_budget_bytes(profile);
        assert_eq!(
            budget,
            profile.managed_budget_bytes.unwrap() - profile.operation_budget_bytes
        );
        assert!(budget / CONNECTION_THREAD_FOOTPRINT_BYTES >= 64);
    }

    #[test]
    fn permits_reserve_and_release_network_memory() {
        let admission = admission_with_limit(256 * MIB);
        let permit = admission.acquire(2, AdmissionPriority::Gateway);
        assert_eq!(admission.governor.reserved_bytes(), 2 * CONNECTION_THREAD_FOOTPRINT_BYTES);
        drop(permit);
        assert_eq!(admission.governor.reserved_bytes(), 0);
    }

    #[test]
    fn saturated_admission_times_out_then_recovers() {
        let admission = admission_with_limit(256 * MIB);
        let capacity = admission.budget_bytes / CONNECTION_THREAD_FOOTPRINT_BYTES;
        let held = (0..capacity)
            .map(|_| admission.acquire(1, AdmissionPriority::Local))
            .collect::<Vec<_>>();
        let deadline = Instant::now() + Duration::from_millis(50);
        assert!(admission
            .acquire_until(1, AdmissionPriority::Gateway, Some(deadline))
            .is_none());
        assert!(lock_unpoisoned(&admission.state).waiting.is_empty());
        drop(held);
        assert!(admission
            .acquire_until(1, AdmissionPriority::Gateway, Some(Instant::now()))
            .is_some());
    }

    #[test]
    fn waiter_is_admitted_when_a_permit_is_released() {
        let admission = admission_with_limit(256 * MIB);
        let capacity = admission.budget_bytes / CONNECTION_THREAD_FOOTPRINT_BYTES;
        let mut held = (0..capacity)
            .map(|_| admission.acquire(1, AdmissionPriority::Local))
            .collect::<Vec<_>>();
        let waiter = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || {
                admission
                    .acquire_until(1, AdmissionPriority::Gateway, Some(Instant::now() + Duration::from_secs(5)))
                    .is_some()
            })
        };
        thread::sleep(Duration::from_millis(20));
        held.pop();
        assert!(waiter.join().unwrap());
    }

    #[test]
    fn local_waiters_go_before_gateway_waiters() {
        let mut state = AdmissionState::default();
        state.waiting.push_back((1, AdmissionPriority::Gateway));
        state.waiting.push_back((2, AdmissionPriority::Local));
        assert_eq!(state.head(), Some(2));
        state.leave(2);
        assert_eq!(state.head(), Some(1));
    }

    #[test]
    fn idle_node_admits_even_beyond_budget() {
        let admission = Arc::new(ConnectionAdmission {
            budget_bytes: 0,
            ..ConnectionAdmission::new(MemoryGovernor::unlimited())
        });
        let first = admission.acquire_until(1, AdmissionPriority::Local, Some(Instant::now()));
        assert!(first.is_some());
        assert!(admission
            .acquire_until(1, AdmissionPriority::Local, Some(Instant::now()))
            .is_none());
    }
}
