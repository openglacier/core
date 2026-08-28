//! Compact operation routing for the event-first core.
mod catalog;
mod definition;
mod event;
mod payload;
mod request;
mod response;
mod router;
//use crate::error::{Error, Result};
pub use catalog::*;
pub use catalog::{
    operation_by_name, AccessPolicy, OperationDescriptor, OperationKind, OPERATION_CATALOG,
};
pub use event::{Audience, Event};
pub use payload::*;
pub use request::{decode_operation_request, IncomingRequest, OperationRequest};
pub use response::OperationResponse;
pub use router::{OperationRouter, Routed, RoutedOperation};
