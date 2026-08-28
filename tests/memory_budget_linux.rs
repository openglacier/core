#![cfg_attr(rustfmt, rustfmt_skip)]
#![cfg(target_os = "linux")]

use og_core::{MemoryGovernor, MemoryProfile, MemoryProfileConfig, WorkloadClass};
#[test] fn linux_process_metrics_are_readable() { let governor = MemoryGovernor::with_process_limit(256 * 1024 * 1024); let process = governor .process_memory_snapshot() .expect("/proc/self/smaps_rollup must be readable on Linux"); assert!(process.rss_bytes > 0); assert!(process.anonymous_bytes <= process.rss_bytes); }
#[test] fn standard_profiles_are_selected_at_documented_limits() { let gib = 1024 * 1024 * 1024; assert_eq!( MemoryProfileConfig::for_limit(256 * 1024 * 1024).profile, MemoryProfile::Mib256 ); assert_eq!( MemoryProfileConfig::for_limit(gib).profile, MemoryProfile::Gib1 ); assert_eq!( MemoryProfileConfig::for_limit(8 * gib).profile, MemoryProfile::Gib8 ); assert_eq!( MemoryProfileConfig::for_limit(16 * gib).profile, MemoryProfile::Gib16 ); assert_eq!( MemoryProfileConfig::for_limit(32 * gib).profile, MemoryProfile::Gib32 ); }
#[test] fn profile_256m_rejects_concurrent_sort_and_import_envelopes() { let governor = MemoryGovernor::with_process_limit(256 * 1024 * 1024); let _sort = governor .admit(WorkloadClass::Query, governor.profile().query_budget_bytes) .expect("first heavy operation must be admitted"); assert!(governor .admit( WorkloadClass::Import, governor.profile().import_budget_bytes ) .is_err()); }
