# Node Fabric V2 — single Gateway port

The Gateway now exposes Hub clients and outbound Core nodes on one HTTP server:

- `/ws` — Hub/browser WebSocket
- `/node` — Core node WebSocket

Core modes:

- `OGD_BIND=127.0.0.1:7878` — historical direct TCP listener
- `OGD_BIND=gw+insecure://127.0.0.1:3000` — outbound plain WebSocket for local development
- `OGD_BIND=gw://gateway.openglacier.org` — outbound secure WebSocket (`wss://gateway.openglacier.org/node`)

Gateway configuration:

```bash
OG_GATEWAY_MODE=gateway
OG_GATEWAY_BIND=0.0.0.0:3000
node src/index.js
```

Historical mode remains:

```bash
OG_GATEWAY_MODE=listen
OG_GATEWAY_BIND=0.0.0.0:3000
OGD_CORE_HOST=127.0.0.1
OGD_CORE_PORT=7878
node src/index.js
```

`OG_GATEWAY_BIND` always means the HTTP server bind. `OG_GATEWAY_MODE` controls whether the Gateway uses a local direct Core or outbound nodes.

The dedicated node TCP listener/port from V1 is removed. The existing MessagePack node protocol is carried one message per binary WebSocket frame. Reverse Core sessions use additional `/node` WebSockets and bridge the existing Core byte protocol, so auth/event/file semantics remain unchanged.

## Rust dependency

Merge `NODE_FABRIC_WEBSOCKET_DEPENDENCIES.toml` into the real `Cargo.toml` because the source-only archive supplied to this session did not include the manifest.
