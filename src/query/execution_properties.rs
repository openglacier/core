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
}

impl ExecutionProperties<'_> {
    #[must_use]
    pub const fn writes(self) -> bool {
        matches!(self.effect, Effect::Write)
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
