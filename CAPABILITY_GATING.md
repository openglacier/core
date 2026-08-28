# OGD service capability gating

`OGD_NODE_CAPABILITIES` is now executable configuration, not only Node Fabric metadata.

The built-in `OperationRouter` is created with the node's enabled service capabilities and rejects an operation before payload execution when its required service is disabled. The stable wire error is:

- code: `capability.unavailable`

Service mapping:

- `auth`: authentication operations; identity/device operations additionally require the internal database service surface.
- `database`: `query.execute`, collections/storage/backup and persisted control-plane operations (permissions, sharing, places, apps).
- `files`: every `file.*` operation.
- `events`: `events.subscribe`.
- `data.import`: `data.analyze` and `data.import` (also require `database`).

`core.health` and `ping` remain available so a node can always be diagnosed. `core.health` returns the effective capability list.

Startup is reduced as well:

- without `files`, the Files data directory is not created;
- without `auth`, the bootstrap admin is not created and no bootstrap password is required;
- without `database`, built-in Apps and system Resources instances are not bootstrapped;
- without `events`, heartbeat publication and authenticated event keepalive are disabled.

The storage engine is still instantiated even on a Files-only node because enabled services may use internal collections for their own metadata. `database` controls the externally exposed database/query/control-plane service, not whether an internal storage engine exists.

Unknown capability names in `OGD_NODE_CAPABILITIES` now fail startup instead of being silently advertised.
