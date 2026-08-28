# Node Fabric V1

This build keeps the existing `OGD_BIND=IP:PORT` listener mode and adds an outbound gateway mode.

## Core modes

Existing listener mode is unchanged:

```bash
OGD_BIND=127.0.0.1:7878 ./ogd
```

Outbound node mode:

```bash
OGD_BIND=gw://gateway.openglacier.org \
OGD_NODE_IDENTITY=identity-node-acme \
OGD_NODE_CAPABILITIES=auth,database,files,events \
OGD_GATEWAY_TOKEN=change-me \
./ogd
```

`gw://host` uses the Gateway node-fabric port `7880` by default. `gw://host:port` is also accepted.

The outbound connection is a control channel only. When Gateway needs a client session it asks the node to open a dedicated reverse channel. The Core then opens another outbound TCP connection and serves the existing og-core protocol on it. This deliberately preserves the current one-authentication-context-per-Core-socket model, raw file transfers, event subscriptions and all existing operation handling without multiplexing identities inside og-core.

## Capabilities

`OGD_NODE_CAPABILITIES` is a comma-separated declaration. V1 defaults to:

- `auth`
- `database`
- `files`
- `events`

These are declared capabilities, not governance assignments. Place/global assignment policy is the next layer and must determine the effective capabilities/scopes.

## Node identity

`OGD_NODE_IDENTITY` announces the OpenGlacier identity intended for the node. In this transport V1 it is metadata; it is not yet cryptographically bound to the node handshake. An optional shared transport token can protect the node ingress during this bootstrap phase. The governance iteration should replace/augment that bootstrap trust with normal OpenGlacier node/service identity authentication.
