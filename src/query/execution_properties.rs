//! Shared physical execution properties derived from planned operators.

use std::sync::Arc;

use super::{ExpressionFieldPath, SortKey};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Flow {
    Streaming,
    GovernedBlocking,
    Specialized,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CardinalityEffect {
    Preserve,
    Reduce,
    Expand,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Bound {
    Unknown,
    AtMost(usize),
    Exact(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Shape {
    Linear,
    Scalar,
    Matrix,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    Row,
    Set,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Order<'a> {
    Unknown,
    Preserved,
    Ordered(&'a Arc<[SortKey]>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Effect {
    ReadOnly,
    Write,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProjectedAccess {
    None,
    Stage,
    Consumer,
}

/// Whether an operator semantically requires a fully materialized document.
///
/// `Deferred` means the operator can preserve or consume a projected row
/// representation even when the current executor has not yet implemented that
/// physical path for every composition. Keeping this separate from
/// [`ProjectedAccess`] lets planning grow capabilities without encoding stage
/// identities in storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Materialization {
    Required,
    Deferred,
}

/// Whether source projections consumed by this operator are safe to reuse.
///
/// Reuse applies to immutable source values, never to an operator's derived
/// result. A filter can therefore consume a reusable projection even when its
/// predicate itself is evaluated at runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProjectionReuse {
    None,
    Reusable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fields<'a> {
    Unknown,
    Preserved,
    Projected(&'a Arc<[ExpressionFieldPath]>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExecutionProperties<'a> {
    pub flow: Flow,
    pub cardinality: CardinalityEffect,
    pub bound: Bound,
    pub order: Order<'a>,
    pub fields: Fields<'a>,
    pub shape: Shape,
    pub scope: Scope,
    pub effect: Effect,
    pub projected_access: ProjectedAccess,
    pub materialization: Materialization,
    pub projection_reuse: ProjectionReuse,
}

impl ExecutionProperties<'_> {
    #[must_use]
    pub const fn writes(self) -> bool {
        matches!(self.effect, Effect::Write)
    }

    #[must_use]
    pub const fn defers_materialization(self) -> bool {
        matches!(self.materialization, Materialization::Deferred)
    }

    #[must_use]
    pub const fn reuses_projection(self) -> bool {
        matches!(self.projection_reuse, ProjectionReuse::Reusable)
    }

    #[must_use]
    pub const fn closes_linear_pipeline(self) -> bool {
        self.writes() || !matches!(self.shape, Shape::Linear)
    }

    /// Returns a generic downstream row bound when this stage is a pure,
    /// streaming, order/field-preserving linear reducer.
    ///
    /// Today `Limit` is the only operator that satisfies this contract. The
    /// executor no longer needs to know its identity to consume the bound.
    #[must_use]
    pub const fn linear_bound(self) -> Option<usize> {
        if !matches!(self.flow, Flow::Streaming)
            || !matches!(self.cardinality, CardinalityEffect::Reduce)
            || !matches!(self.order, Order::Preserved)
            || !matches!(self.fields, Fields::Preserved)
            || !matches!(self.shape, Shape::Linear)
            || !matches!(self.effect, Effect::ReadOnly)
        {
            return None;
        }
        match self.bound {
            Bound::AtMost(value) | Bound::Exact(value) => Some(value),
            Bound::Unknown => None,
        }
    }
}
