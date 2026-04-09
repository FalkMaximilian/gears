//! Typed workflow and activity traits.
//!
//! [`TypedWorkflow`] and [`TypedActivity`] mirror the untyped [`Workflow`] and
//! [`Activity`] traits but use associated `Input` / `Output` types instead of
//! raw `serde_json::Value`. Blanket implementations bridge the two: any struct
//! that implements `TypedWorkflow` automatically implements `Workflow` (and
//! likewise for activities), so registration on the engine is unchanged.
//!
//! # Example
//!
//! ```ignore
//! #[derive(Serialize, Deserialize)]
//! struct GreetInput { name: String }
//!
//! #[derive(Serialize, Deserialize)]
//! struct GreetOutput { message: String }
//!
//! struct GreetActivity;
//!
//! impl TypedActivity for GreetActivity {
//!     type Input = GreetInput;
//!     type Output = GreetOutput;
//!
//!     fn name(&self) -> &'static str { "greet" }
//!
//!     fn execute(&self, _ctx: ActivityContext, input: GreetInput) -> TypedActivityFuture<GreetOutput> {
//!         Box::pin(async move {
//!             Ok(GreetOutput { message: format!("Hello, {}!", input.name) })
//!         })
//!     }
//! }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::context::{ActivityContext, WorkflowContext};
use crate::error::Result;
use crate::traits::{Activity, ActivityFuture, Workflow, WorkflowFuture};

// ── Type aliases ─────────────────────────────────────────────────────────

/// Convenience future alias for [`TypedWorkflow::run`].
pub type TypedWorkflowFuture<O> = Pin<Box<dyn Future<Output = Result<O>> + Send + 'static>>;

/// Convenience future alias for [`TypedActivity::execute`].
pub type TypedActivityFuture<O> = Pin<Box<dyn Future<Output = Result<O>> + Send + 'static>>;

// ── TypedWorkflow ────────────────────────────────────────────────────────

/// A workflow with strongly-typed input and output.
///
/// Implementing this trait automatically provides a [`Workflow`]
/// implementation via blanket impl — register it on the engine the same
/// way you would an untyped workflow.
pub trait TypedWorkflow: Send + Sync + 'static {
    /// Deserialized from the JSON `Value` passed to `engine.start_workflow`.
    type Input: DeserializeOwned + Serialize + Send + 'static;
    /// Serialized to `Value` for the `WorkflowCompleted` event.
    type Output: DeserializeOwned + Serialize + Send + 'static;

    /// Stable identifier — must be unique across registered workflows.
    fn name(&self) -> &'static str;

    /// Entry point, called with a deserialized `Input`.
    fn run(&self, ctx: WorkflowContext, input: Self::Input) -> TypedWorkflowFuture<Self::Output>;
}

impl<T> Workflow for T
where
    T: TypedWorkflow,
{
    fn name(&self) -> &'static str {
        TypedWorkflow::name(self)
    }

    fn run(&self, ctx: WorkflowContext, input: serde_json::Value) -> WorkflowFuture {
        let typed_input = match serde_json::from_value::<T::Input>(input) {
            Ok(v) => v,
            Err(e) => return Box::pin(async move { Err(e.into()) }),
        };
        let fut = TypedWorkflow::run(self, ctx, typed_input);
        Box::pin(async move {
            let output = fut.await?;
            Ok(serde_json::to_value(output)?)
        })
    }
}

// ── TypedActivity ────────────────────────────────────────────────────────

/// An activity with strongly-typed input and output.
///
/// Implementing this trait automatically provides an [`Activity`]
/// implementation via blanket impl — register it on the engine the same
/// way you would an untyped activity.
pub trait TypedActivity: Send + Sync + 'static {
    /// Deserialized from the JSON `Value` passed to `ctx.execute_activity`.
    type Input: DeserializeOwned + Serialize + Send + 'static;
    /// Serialized to `Value` for the `ActivityCompleted` event.
    type Output: DeserializeOwned + Serialize + Send + 'static;

    /// Stable identifier — must be unique across registered activities.
    fn name(&self) -> &'static str;

    /// Execute the activity with a deserialized `Input`.
    fn execute(
        &self,
        ctx: ActivityContext,
        input: Self::Input,
    ) -> TypedActivityFuture<Self::Output>;

    /// Maximum retry attempts. Defaults to 3.
    fn max_attempts(&self) -> u32 {
        3
    }

    /// Base delay before the first retry. Defaults to 1 second.
    fn retry_base_delay(&self) -> Duration {
        Duration::from_secs(1)
    }

    /// Optional per-activity execution deadline. Defaults to `None`.
    fn timeout(&self) -> Option<Duration> {
        None
    }
}

impl<T> Activity for T
where
    T: TypedActivity,
{
    fn name(&self) -> &'static str {
        TypedActivity::name(self)
    }

    fn execute(&self, ctx: ActivityContext, input: serde_json::Value) -> ActivityFuture {
        let typed_input = match serde_json::from_value::<T::Input>(input) {
            Ok(v) => v,
            Err(e) => return Box::pin(async move { Err(e.into()) }),
        };
        let fut = TypedActivity::execute(self, ctx, typed_input);
        Box::pin(async move {
            let output = fut.await?;
            Ok(serde_json::to_value(output)?)
        })
    }

    fn max_attempts(&self) -> u32 {
        TypedActivity::max_attempts(self)
    }

    fn retry_base_delay(&self) -> Duration {
        TypedActivity::retry_base_delay(self)
    }

    fn timeout(&self) -> Option<Duration> {
        TypedActivity::timeout(self)
    }
}
