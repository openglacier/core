# Node Governance V1

## Node identity

Outbound Core nodes can load a real OpenGlacier `.ogid` credential:

```bash
OGD_BIND=gw+insecure://127.0.0.1:3000 \
OGD_NODE_IDENTITY_FILE=/path/to/node.ogid \
OGD_NODE_IDENTITY_PASSWORD='...' \
ogd
```

The Node Fabric hello derives `identityId`, `deviceId` and `publicKey` from the credential and signs a short-lived proof. Gateway verifies possession of the announced Ed25519 key and exposes `identityVerified` in its live node registry. `OGD_NODE_IDENTITY` remains supported as a legacy unverified declaration.

Transport bootstrap token (`OGD_GATEWAY_TOKEN`) is independent of the OpenGlacier identity and may still be used.

## Place resource assignments

Assignments are persisted on the Place as `resourceAssignments`. They are owned by the Place, not by the live Gateway node registry.

Operations:

- `place.resource.list { placeId }`
- `place.resource.set { placeId, nodeIdentityId, capability, role }`
- `place.resource.remove { placeId, nodeIdentityId, capability }`

Roles are `primary`, `replica`, or `provider`.

Only Place Owners may set/remove assignments. The node Identity must already be attached to the Place using the normal Place access/sharing model. There can be only one `primary` assignment per capability on a Place.

Changes emit durable `place.resources.updated` events through the existing Core event outbox.

## system.ressources

`system.ressources` is bootstrapped by Core if it is absent from the external built-in app catalog. This V1 is a shell for the upcoming live Resources UI.

## Security boundary

The Gateway V1 verifies cryptographic possession of the key announced by a node. Validation that this key/device is the currently registered credential for that Identity remains a master/Core responsibility and should be enforced by the capability resolver before routing governed resources.
