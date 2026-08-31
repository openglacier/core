//! Dense operation registry and router.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::protocol::RequestId;

#[cfg(test)]
use crate::access::place::{PlaceRole, RequestedExecutionContext};

use crate::error::{Error, Result};

use super::catalog::*;
use super::definition::operation_definitions;
use super::payload::*;
use super::{catalog::OperationKind, OperationRequest};

/// Request metadata paired with one validated typed payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routed<T> {
    pub id: RequestId,
    pub input: T,
}

impl<T> Routed<T> {
    #[must_use]
    pub const fn new(id: RequestId, input: T) -> Self {
        Self { id, input }
    }
}

/// A validated operation ready for execution.
macro_rules! define_routed_operations {
    ($($constant:ident => $kind:ident, $name:literal, $access:expr, $execution:expr, $handler:expr, $transport:expr, $connection:expr, $payload:ty;)+) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum RoutedOperation {
            $($kind(Routed<$payload>),)+
        }

        impl RoutedOperation {
            #[must_use]
            pub const fn kind(&self) -> OperationKind {
                match self { $(Self::$kind(_) => OperationKind::$kind,)+ }
            }

            #[must_use]
            pub const fn id(&self) -> RequestId {
                match self { $(Self::$kind(routed) => routed.id,)+ }
            }

            #[must_use]
            pub const fn execution_mode(&self) -> ExecutionMode { self.kind().execution_mode() }

            #[must_use]
            pub const fn handler(&self) -> HandlerKind { self.kind().handler() }

            #[must_use]
            pub const fn transport(&self) -> TransportKind { self.kind().transport() }

            #[must_use]
            pub const fn connection(&self) -> ConnectionKind { self.kind().connection() }
        }
    };
}

operation_definitions!(define_routed_operations);

/// Registry-backed operation router.
#[derive(Debug, Clone)]
pub struct OperationRouter {
    operations: BTreeMap<String, OperationKind>,
    builtins: bool,
    service_capabilities: ServiceCapabilities,
}

impl Default for OperationRouter {
    fn default() -> Self {
        Self {
            operations: BTreeMap::new(),
            builtins: true,
            service_capabilities: ServiceCapabilities::ALL,
        }
    }
}

impl OperationRouter {
    /// Creates an empty router, mainly for tests and embedding.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            operations: BTreeMap::new(),
            builtins: false,
            service_capabilities: ServiceCapabilities::ALL,
        }
    }

    /// Creates the built-in router gated by the services enabled on this node.
    #[must_use]
    pub fn for_capabilities(service_capabilities: ServiceCapabilities) -> Self {
        Self {
            operations: BTreeMap::new(),
            builtins: true,
            service_capabilities,
        }
    }

    /// Returns whether an operation is both registered and executable with the
    /// node's current service capabilities.
    #[must_use]
    pub fn is_available(&self, name: &str) -> bool {
        let kind = self.operations.get(name).copied().or_else(|| {
            self.builtins
                .then(|| operation_by_name(name))
                .flatten()
                .map(|entry| entry.kind)
        });
        kind.is_some_and(|kind| {
            self.service_capabilities
                .contains_all(kind.required_capabilities())
        })
    }

    /// Registers an operation name exactly once.
    pub fn register(&mut self, name: impl Into<String>, kind: OperationKind) -> Result<()> {
        let name = name.into();
        if self.operations.contains_key(&name)
            || (self.builtins && operation_by_name(&name).is_some())
        {
            return Err(Error::OperationAlreadyRegistered {
                operation: name.to_owned(),
            });
            /*return Err(OperationError::new(OperationErrorKind::AlreadyRegistered {
                operation: name,
            }));*/
        }
        self.operations.insert(name, kind);
        Ok(())
    }

    /// Returns whether an operation is registered.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.operations.contains_key(name) || (self.builtins && operation_by_name(name).is_some())
    }

    /// Validates and routes one operation request.
    pub fn route(&self, request: OperationRequest) -> Result<RoutedOperation> {
        let kind = self
            .operations
            .get(request.op.as_str())
            .copied()
            .or_else(|| {
                self.builtins
                    .then(|| operation_by_name(request.op.as_str()))
                    .flatten()
                    .map(|entry| entry.kind)
            })
            .ok_or_else(|| Error::OperationNotFound {
                operation: request.op.clone(),
            })?;
        let required = kind.required_capabilities();
        if !self.service_capabilities.contains_all(required) {
            let required = required.names().join(",");
            return Err(Error::CapabilityUnavailable {
                operation: request.op,
                required,
            });
        }
        let id = request.id;
        let data = request.data;

        macro_rules! dispatch_operations {
            ($($constant:ident => $kind:ident, $name:literal, $access:expr, $execution:expr, $handler:expr, $transport:expr, $connection:expr, $payload:ty;)+) => {
                match kind {
                    $(OperationKind::$kind => Ok(RoutedOperation::$kind(
                        decode_routed::<$payload>($constant, id, data)?
                    )),)+
                }
            };
        }

        operation_definitions!(dispatch_operations)
    }
}

fn decode_payload<T>(operation: &str, data: serde_json::Value) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(data).map_err(|error| Error::InvalidOperationPayload {
        operation: operation.to_owned(),
        reason: error.to_string(),
    })
}

fn decode_routed<T>(operation: &str, id: RequestId, data: serde_json::Value) -> Result<Routed<T>>
where
    T: for<'de> Deserialize<'de> + OperationPayload,
{
    let mut input = decode_payload::<T>(operation, data)?;
    input.validate(operation)?;
    Ok(Routed::new(id, input))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn default_router_exposes_builtin_operations() {
        let router = OperationRouter::default();
        for operation in [
            QUERY_EXECUTE,
            AUTH_BEGIN,
            AUTH_COMPLETE,
            EVENTS_SUBSCRIBE,
            IDENTITY_REGISTER,
            IDENTITY_OPEN,
            IDENTITY_GET,
            IDENTITY_RENEW,
            DEVICE_REGISTER,
            DEVICE_REVOKE,
            PERMISSION_GRANT,
            PERMISSION_REVOKE,
            SHARING_CREATE,
            SHARING_UPDATE,
            SHARING_DELETE,
            PLACE_CREATE,
            PLACE_LIST,
            PLACE_GET,
            PLACE_DELETE,
            PLACE_PUBLIC_SET,
            APP_CREATE,
            APP_LIST,
            APP_GET,
            APP_UPDATE,
            APP_DELETE,
            APP_INSTANCE_CREATE,
            APP_INSTANCE_LIST,
            APP_INSTANCE_REMOVE,
        ] {
            assert!(router.contains(operation));
        }
    }

    #[test]
    fn capability_gated_router_rejects_disabled_services() {
        let router = OperationRouter::for_capabilities(
            ServiceCapabilities::NONE
                .with(ServiceCapability::Auth)
                .with(ServiceCapability::Database)
                .with(ServiceCapability::Events),
        );
        assert!(router.is_available(QUERY_EXECUTE));
        assert!(!router.is_available(FILE_LIST));
        let error = router
            .route(OperationRequest::new(
                9,
                FILE_LIST,
                serde_json::json!({"placeId":"p","instanceId":"i"}),
            ))
            .unwrap_err();
        assert_eq!(error.code(), "capability.unavailable");
    }

    #[test]
    fn query_execute_routes_its_query() {
        let routed = OperationRouter::default()
            .route(OperationRequest::query(7, "on users | limit 1"))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::QueryExecute(Routed::new(
                RequestId::Number(7),
                QueryExecuteInput {
                    query: "on users | limit 1".to_owned(),
                    context: None
                },
            ))
        );
    }

    #[test]
    fn identity_register_applies_crypto_defaults() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                3,
                IDENTITY_REGISTER,
                json!({
                    "identityId": "identity-a",
                    "publicKey": "ZmFrZQ=="
                }),
            ))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::IdentityRegister(Routed::new(
                RequestId::Number(3),
                IdentityRegisterInput {
                    identity_id: "identity-a".to_owned(),
                    public_key: "ZmFrZQ==".to_owned(),
                    algorithm: "ed25519".to_owned(),
                    encoding: "spki-der".to_owned(),
                    created_at: None,
                },
            ))
        );
    }

    #[test]
    fn identity_renew_is_self_scoped_payload() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                4,
                IDENTITY_RENEW,
                json!({"deviceId": "device-next", "publicKey": "ZmFrZQ=="}),
            ))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::IdentityRenew(Routed::new(
                RequestId::Number(4),
                IdentityRenewInput {
                    device_id: Some("device-next".to_owned()),
                    public_key: Some("ZmFrZQ==".to_owned()),
                    password: None
                },
            ))
        );
    }

    #[test]
    fn device_register_requires_identity_and_device_ids() {
        let error = OperationRouter::default()
            .route(OperationRequest::new(
                4,
                DEVICE_REGISTER,
                json!({
                    "deviceId": "",
                    "identityId": "identity-a",
                    "publicKey": "ZmFrZQ=="
                }),
            ))
            .unwrap_err();
        assert_eq!(error.code(), "operation.invalid_payload");
    }

    #[test]
    fn device_revoke_routes_timestamp() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                5,
                DEVICE_REVOKE,
                json!({"deviceId": "device-a", "revokedAt": 42}),
            ))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::DeviceRevoke(Routed::new(
                RequestId::Number(5),
                DeviceRevokeInput {
                    device_id: "device-a".to_owned(),
                    revoked_at: Some(42)
                },
            ))
        );
    }

    #[test]
    fn auth_begin_routes_identity_and_device() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                8,
                AUTH_BEGIN,
                json!({"identityId": "identity-a", "deviceId": "device-a"}),
            ))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::AuthBegin(Routed::new(
                RequestId::Number(8),
                AuthBeginInput {
                    identity_id: "identity-a".to_owned(),
                    device_id: "device-a".to_owned()
                },
            ))
        );
    }

    #[test]
    fn auth_complete_requires_signature() {
        let error = OperationRouter::default()
            .route(OperationRequest::new(
                9,
                AUTH_COMPLETE,
                json!({"challengeId": "challenge-a", "signature": ""}),
            ))
            .unwrap_err();
        assert_eq!(error.code(), "operation.invalid_payload");
    }

    #[test]
    fn permission_grant_routes_rule() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                12,
                PERMISSION_GRANT,
                json!({"identityId":"identity-a","action":"query.read","resource":"users"}),
            ))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::PermissionGrant(Routed::new(
                RequestId::Number(12),
                PermissionGrantInput {
                    identity_id: "identity-a".to_owned(),
                    action: "query.read".to_owned(),
                    resource: "users".to_owned(),
                    created_at: None,
                },
            ))
        );
    }

    #[test]
    fn sharing_create_normalizes_permissions() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                21,
                SHARING_CREATE,
                json!({
                    "sharingId": "sharing-a",
                    "owner": "identity-a",
                    "target": "identity-b",
                    "permissions": ["events", "files", "events"]
                }),
            ))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::SharingCreate(Routed::new(
                RequestId::Number(21),
                SharingCreateInput {
                    sharing_id: "sharing-a".to_owned(),
                    owner: "identity-a".to_owned(),
                    target: "identity-b".to_owned(),
                    permissions: vec!["events".to_owned(), "files".to_owned()],
                    state: "accepted".to_owned(),
                    created_at: None,
                },
            ))
        );
    }

    #[test]
    fn sharing_update_requires_a_change() {
        let error = OperationRouter::default()
            .route(OperationRequest::new(
                22,
                SHARING_UPDATE,
                json!({"sharingId": "sharing-a"}),
            ))
            .unwrap_err();
        assert_eq!(error.code(), "operation.invalid_payload");
    }

    #[test]
    fn enrollment_operations_route_without_authentication_context() {
        let begin = OperationRouter::default()
            .route(OperationRequest::new(
                30,
                AUTH_ENROLL_BEGIN,
                json!({
                    "identityId":"identity-a", "identityPublicKey":"identity-key",
                    "deviceId":"device-a", "devicePublicKey":"device-key"
                }),
            ))
            .unwrap();
        assert!(matches!(
            begin,
            RoutedOperation::AuthEnrollBegin(Routed {
                id: RequestId::Number(30),
                ..
            })
        ));
        let complete = OperationRouter::default()
            .route(OperationRequest::new(
                31,
                AUTH_ENROLL_COMPLETE,
                json!({
                    "challengeId":"challenge-a", "signature":"signature-a"
                }),
            ))
            .unwrap();
        assert!(matches!(
            complete,
            RoutedOperation::AuthEnrollComplete(Routed {
                id: RequestId::Number(31),
                ..
            })
        ));
    }

    #[test]
    fn place_create_routes_system_context_fields() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                40,
                PLACE_CREATE,
                json!({"name":"Workshop"}),
            ))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::PlaceCreate(Routed::new(
                RequestId::Number(40),
                PlaceCreateInput {
                    name: "Workshop".to_owned(),
                    mood: String::new(),
                    public_access: None,
                    created_at: None
                },
            ))
        );
    }

    #[test]
    fn place_update_routes_presentation_and_order() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                41,
                PLACE_UPDATE,
                json!({
                    "placeId":"place-a",
                    "name":"Studio",
                    "title":"Design together",
                    "subtitle":"A shared workspace",
                    "colorScheme":"sage",
                    "appOrder":["instance-b","instance-a"]
                }),
            ))
            .unwrap();
        assert!(matches!(
            routed,
            RoutedOperation::PlaceUpdate(Routed {
                id: RequestId::Number(41),
                input: PlaceUpdateInput { ref place_id, ref color_scheme, ref app_order, .. },
            }) if place_id == "place-a" && color_scheme.as_deref() == Some("sage") && app_order.as_deref() == Some(&["instance-b".to_owned(), "instance-a".to_owned()][..])
        ));
    }

    #[test]
    fn place_public_set_routes_access_mode() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                41,
                PLACE_PUBLIC_SET,
                json!({"placeId":"place-a","publicAccess":"readonly"}),
            ))
            .unwrap();
        assert!(matches!(
            routed,
            RoutedOperation::PlacePublicSet(Routed {
                id: RequestId::Number(41),
                input: PlacePublicSetInput { ref place_id, public_access: Some(crate::access::place::PublicAccess::Readonly) },
            }) if place_id == "place-a"
        ));
    }

    #[test]
    fn place_access_set_routes_role() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                42,
                PLACE_ACCESS_SET,
                json!({
                    "placeId":"place-a",
                    "identityId":"identity-b",
                    "role":"resident"
                }),
            ))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::PlaceAccessSet(Routed::new(
                RequestId::Number(42),
                PlaceAccessSetInput {
                    place_id: "place-a".to_owned(),
                    identity_id: "identity-b".to_owned(),
                    role: PlaceRole::Resident,
                },
            ))
        );
    }

    #[test]
    fn app_create_routes_opaque_definition() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                42,
                APP_CREATE,
                json!({
                    "appId":"local.inventory",
                    "name":"Inventory",
                    "version":"1.0.0",
                    "definition":{"views":{"tile":[],"full":[]}}
                }),
            ))
            .unwrap();
        assert_eq!(
            routed,
            RoutedOperation::AppCreate(Routed::new(
                RequestId::Number(42),
                AppCreateInput {
                    app_id: "local.inventory".to_owned(),
                    place_id: None,
                    name: "Inventory".to_owned(),
                    version: "1.0.0".to_owned(),
                    definition: json!({"views":{"tile":[],"full":[]}}),
                    created_at: None,
                },
            ))
        );
    }

    #[test]
    fn app_update_rejects_non_object_definition() {
        let error = OperationRouter::default()
            .route(OperationRequest::new(
                42,
                APP_UPDATE,
                json!({
                    "appId":"local.inventory",
                    "name":"Inventory",
                    "version":"1.0.1",
                    "definition":[]
                }),
            ))
            .unwrap_err();
        assert_eq!(error.code(), "operation.invalid_payload");
    }

    #[test]
    fn app_instance_create_routes_place_binding() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                41,
                APP_INSTANCE_CREATE,
                json!({
                    "placeId":"place-a",
                    "appId":"org.openglacier.inventory",
                    "name":"Main stock",
                    "config":{"density":"comfortable"}
                }),
            ))
            .unwrap();
        assert!(matches!(
            routed,
            RoutedOperation::AppInstanceCreate(Routed {
                id: RequestId::Number(41),
                input: AppInstanceCreateInput { ref place_id, .. },
            }) if place_id == "place-a"
        ));
    }

    #[test]
    fn unknown_operation_is_rejected() {
        let error = OperationRouter::default()
            .route(OperationRequest::new(1, "unknown.run", json!({})))
            .unwrap_err();
        assert_eq!(error.code(), "operation.not_found");
    }

    #[test]
    fn duplicate_registration_is_rejected() {
        let mut router = OperationRouter::empty();
        router
            .register("query.execute", OperationKind::QueryExecute)
            .unwrap();
        let error = router
            .register("query.execute", OperationKind::QueryExecute)
            .unwrap_err();
        assert_eq!(error.code(), "operation.already_registered");
    }

    #[test]
    fn query_execute_accepts_optional_place_context() {
        let router = OperationRouter::default();
        let request = OperationRequest::new(
            91,
            QUERY_EXECUTE,
            serde_json::json!({
                "query": "on inventory | limit 1",
                "context": {
                    "placeId": "place-workshop",
                    "appInstanceId": "inventory-main"
                }
            }),
        );
        let routed_request = router.route(request).unwrap();
        assert!(matches!(
            routed_request,
            RoutedOperation::QueryExecute(Routed {
                id: RequestId::Number(91),
                input: QueryExecuteInput { query, context: Some(RequestedExecutionContext { place_id, app_instance_id }) },
            }) if query == "on inventory | limit 1"
                && place_id == "place-workshop"
                && app_instance_id == "inventory-main"
        ));
    }
    #[test]
    fn file_operations_require_app_scope() {
        let routed = OperationRouter::default()
            .route(OperationRequest::new(
                92,
                FILE_MKDIR,
                json!({"placeId":"place-a","instanceId":"files-a","name":"Documents"}),
            ))
            .unwrap();
        assert!(
            matches!(routed,RoutedOperation::FileMkdir(Routed { id: RequestId::Number(92), input: FileMkdirInput { ref place_id, ref instance_id, ref name, .. } })
            if place_id=="place-a" && instance_id=="files-a" && name=="Documents")
        );

        let error = OperationRouter::default()
            .route(OperationRequest::new(
                93,
                FILE_STAT,
                json!({"placeId":"place-a","instanceId":"files-a","fileId":""}),
            ))
            .unwrap_err();
        assert_eq!(error.code(), "operation.invalid_payload");
    }
    #[test]
    fn file_stream_operations_route_without_payload_materialization() {
        let read=OperationRouter::default().route(OperationRequest::new(
            94, FILE_READ,
            json!({"placeId":"place-a","instanceId":"files-a","fileId":"file-a","offset":10,"length":20})
        )).unwrap();
        assert!(matches!(
            read,
            RoutedOperation::FileRead(Routed {
                id: RequestId::Number(94),
                input: FileReadInput {
                    offset: 10,
                    length: Some(20),
                    ..
                },
            })
        ));

        let write=OperationRouter::default().route(OperationRequest::new(
            95, FILE_WRITE,
            json!({"placeId":"place-a","instanceId":"files-a","parentId":null,"name":"a.bin","contentType":"application/octet-stream","size":1048576})
        )).unwrap();
        assert!(
            matches!(write,RoutedOperation::FileWrite(Routed { id: RequestId::Number(95), input: FileWriteInput { size: 1048576, ref name, .. } }) if name.as_deref()==Some("a.bin"))
        );
    }
}
