//! Optional metrics instrumentation, gated behind the `metrics` Cargo feature.
//!
//! When the feature is enabled, gears records counters, gauges and
//! histograms via the [`metrics`](https://docs.rs/metrics) facade. Users
//! install their own exporter (e.g. `metrics-exporter-prometheus`).
//!
//! When the feature is disabled, all functions are no-ops.

/// Total workflows started, labelled by workflow name.
pub const WORKFLOW_STARTED: &str = "gears_workflow_started_total";
/// Total workflows completed successfully.
pub const WORKFLOW_COMPLETED: &str = "gears_workflow_completed_total";
/// Total workflows that failed.
pub const WORKFLOW_FAILED: &str = "gears_workflow_failed_total";
/// Total workflows cancelled.
pub const WORKFLOW_CANCELLED: &str = "gears_workflow_cancelled_total";
/// Currently active (in-progress) workflows.
pub const WORKFLOW_ACTIVE: &str = "gears_workflow_active";
/// Total activity executions started.
pub const ACTIVITY_STARTED: &str = "gears_activity_started_total";
/// Total activity executions completed.
pub const ACTIVITY_COMPLETED: &str = "gears_activity_completed_total";
/// Total activity retry attempts (not counting the first attempt).
pub const ACTIVITY_RETRIES: &str = "gears_activity_retries_total";
/// Activity execution duration in seconds.
pub const ACTIVITY_DURATION: &str = "gears_activity_duration_seconds";

// ── Instrumentation helpers ──────────────────────────────────────────────

#[cfg(feature = "metrics")]
pub(crate) fn inc_workflow_started(workflow_name: &str) {
    metrics::counter!(WORKFLOW_STARTED, "workflow" => workflow_name.to_string()).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_workflow_started(_: &str) {}

#[cfg(feature = "metrics")]
pub(crate) fn inc_workflow_completed(workflow_name: &str) {
    metrics::counter!(WORKFLOW_COMPLETED, "workflow" => workflow_name.to_string()).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_workflow_completed(_: &str) {}

#[cfg(feature = "metrics")]
pub(crate) fn inc_workflow_failed(workflow_name: &str) {
    metrics::counter!(WORKFLOW_FAILED, "workflow" => workflow_name.to_string()).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_workflow_failed(_: &str) {}

#[cfg(feature = "metrics")]
pub(crate) fn inc_workflow_cancelled(workflow_name: &str) {
    metrics::counter!(WORKFLOW_CANCELLED, "workflow" => workflow_name.to_string()).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_workflow_cancelled(_: &str) {}

#[cfg(feature = "metrics")]
pub(crate) fn inc_workflow_active() {
    metrics::gauge!(WORKFLOW_ACTIVE).increment(1.0);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_workflow_active() {}

#[cfg(feature = "metrics")]
pub(crate) fn dec_workflow_active() {
    metrics::gauge!(WORKFLOW_ACTIVE).decrement(1.0);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn dec_workflow_active() {}

#[cfg(feature = "metrics")]
pub(crate) fn inc_activity_started(activity_name: &str) {
    metrics::counter!(ACTIVITY_STARTED, "activity" => activity_name.to_string()).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_activity_started(_: &str) {}

#[cfg(feature = "metrics")]
pub(crate) fn inc_activity_completed(activity_name: &str) {
    metrics::counter!(ACTIVITY_COMPLETED, "activity" => activity_name.to_string()).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_activity_completed(_: &str) {}

#[cfg(feature = "metrics")]
pub(crate) fn inc_activity_retries(activity_name: &str) {
    metrics::counter!(ACTIVITY_RETRIES, "activity" => activity_name.to_string()).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_activity_retries(_: &str) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_activity_duration(activity_name: &str, duration_secs: f64) {
    metrics::histogram!(ACTIVITY_DURATION, "activity" => activity_name.to_string())
        .record(duration_secs);
}
#[cfg(not(feature = "metrics"))]
pub(crate) fn record_activity_duration(_: &str, _: f64) {}
