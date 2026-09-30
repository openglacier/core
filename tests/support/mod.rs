//! Runtimes shared by the engine integration tests.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code)]

use std::sync::Arc;

use og_core::{
    query::{CustomOperatorResult, ExecutionError, ExecutionResult, ExecutionRuntime, Expression, SetAssignment, StageName},
    Document,
};

/// A runtime whose `where` and `set` are the given closures. With
/// `passthrough`, `load` and custom stages keep the row unchanged; otherwise
/// they are unsupported, like every other operator.
pub struct TestRuntime<P, S> {
    pub predicate: P,
    pub set: S,
    pub passthrough: bool,
}

impl<P, S> ExecutionRuntime for TestRuntime<P, S>
where
    P: Fn(&Expression, &Document) -> ExecutionResult<bool> + Send + Sync,
    S: Fn(&[SetAssignment], &Document) -> ExecutionResult<Arc<Document>> + Send + Sync,
{
    fn evaluate_predicate(&self, expression: &Expression, document: &Document) -> ExecutionResult<bool> {
        (self.predicate)(expression, document)
    }

    fn apply_set(&self, assignments: &[SetAssignment], document: &Document) -> ExecutionResult<Arc<Document>> {
        (self.set)(assignments, document)
    }

    fn apply_load(&self, target: &str, document: &Document) -> ExecutionResult<Arc<Document>> {
        if !self.passthrough {
            return Err(ExecutionError::unsupported_operator("load", format!("load target {target:?} has no runtime implementation")));
        }
        Ok(Arc::new(document.clone()))
    }

    fn apply_custom(&self, stage: &StageName, _arguments: &str, _writes: bool, _document: &Document) -> ExecutionResult<CustomOperatorResult> {
        if !self.passthrough {
            return Err(ExecutionError::unsupported_operator(stage.as_str(), "custom operator has no runtime implementation"));
        }
        Ok(CustomOperatorResult::Keep)
    }
}

/// A runtime that keeps every row and leaves every document unchanged.
pub fn passthrough_runtime() -> Arc<dyn ExecutionRuntime> {
    Arc::new(TestRuntime {
        predicate: |_: &Expression, _: &Document| Ok(true),
        set: |_: &[SetAssignment], document: &Document| Ok(Arc::new(document.clone())),
        passthrough: true,
    })
}

/// A runtime with the given `where` and `set` and no other operator.
pub fn runtime<P, S>(predicate: P, set: S) -> Arc<dyn ExecutionRuntime>
where
    P: Fn(&Expression, &Document) -> ExecutionResult<bool> + Send + Sync + 'static,
    S: Fn(&[SetAssignment], &Document) -> ExecutionResult<Arc<Document>> + Send + Sync + 'static,
{
    Arc::new(TestRuntime { predicate, set, passthrough: false })
}
