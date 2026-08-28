# Node Fabric V2 compile fix

Two omissions from the previous pack are fixed:

1. `spawn_serving_connection` is restored in `src/bin/ogd.rs` (it is used by the historical IP:PORT listener and had been accidentally dropped during the single-port merge).
2. The WebSocket client requires `tungstenite` in the real `Cargo.toml`:

```toml
tungstenite = { version = "0.24", features = ["native-tls"] }
```

The uploaded source archive does not include the project `Cargo.toml`, so the dependency cannot be injected into it automatically. Add the line under `[dependencies]` in the actual og-core repository.
