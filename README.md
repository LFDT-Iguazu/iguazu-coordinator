# iguazu-coordinator

Apache 2.0 sidecar binary for the Iguazu network. Runs alongside the Prysm-based `coordinator` node (GPL-3.0) and owns all asynchronous Iguazu-specific work — starting with the DAG finalization loop.

`iguazu-coordinator` has **zero dependency on coordinator / Prysm code**. It communicates with the coordinator exclusively over gRPC, which makes the two processes a "mere aggregation" under GPL-3.0 §5.

---

## Architecture

```
coordinator (GPL-3.0)           iguazu-coordinator (Apache 2.0)
┌───────────────────────┐       ┌────────────────────────────┐
│  beacon-chain node    │       │  cmd/main.go               │
│                       │◄─────►│  finalization/service.go   │
│  CoordinatorInternal  │ gRPC  │    StreamNewHeads           │
│  gRPC service:        │       │    GetFinalizationParams    │
│   StreamNewHeads      │       │    SubmitFinalizationResult │
│   GetFinalizationParams│      │                            │
│   SubmitFinalization  │       │  finalization/dag_client.go│
│     Result            │       │    dag_finalize (JSON-RPC) │
└───────────────────────┘       └──────────┬─────────────────┘
                                           │ HTTP JSON-RPC
                                    ┌──────▼──────┐
                                    │  gwat / iguazu-engine │
                                    └─────────────┘
```

### DAG finalization loop

1. Subscribe to `StreamNewHeads` on startup.
2. For each new canonical head received from the coordinator:
   - Call `GetFinalizationParams(block_root)` → receive `FinalizationParamsResponse`.
   - Call gwat `dag_finalize` with the params.
   - Call `SubmitFinalizationResult` with the gwat outcome so the coordinator can cache the new gwat coordinated checkpoint.
3. On any error: log and reconnect after 5 s.

---

## Package layout

```
iguazu-coordinator/
├── cmd/
│   └── main.go                  # CLI entry point
├── finalization/
│   ├── dag_client.go            # gwat JSON-RPC client (dag_finalize, dag_coordinatedState)
│   ├── service.go               # DAG finalization loop
│   └── service_test.go
└── proto/
    └── coordpb/
        ├── coordinator_internal.pb.go       # proto message types (Apache 2.0 copy)
        └── coordinator_internal_grpc.pb.go  # gRPC client bindings
```

---

## Build

```bash
go build -o iguazu-coordinator ./cmd/
```

Requires Go 1.21+.

---

## Run

```bash
./iguazu-coordinator \
  --coordinator-grpc localhost:4000 \
  --gwat-http       http://localhost:8545 \
  --log-level       info
```

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--coordinator-grpc` | `WF_COORDINATOR_GRPC` | `localhost:4000` | Coordinator gRPC endpoint (`host:port` or `unix:///path`) |
| `--gwat-http` | `WF_GWAT_HTTP` | `http://localhost:8545` | gwat JSON-RPC HTTP endpoint |
| `--log-level` | `WF_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |

The process exits cleanly on `SIGINT` / `SIGTERM`.

---

## Test

```bash
go test ./...
```

---

## Dependencies

| Module | License | Role |
|--------|---------|------|
| `github.com/LFDT-Iguazu/iguazu-types` | Apache 2.0 | dag finalization types (`dag_finalize` params/result) |
| `google.golang.org/grpc` | Apache 2.0 | gRPC transport |
| `google.golang.org/protobuf` | BSD-3-Clause | protobuf runtime |
| `github.com/sirupsen/logrus` | MIT | structured logging |
| `github.com/urfave/cli/v2` | MIT | CLI flag parsing |
| `github.com/pkg/errors` | BSD-2-Clause | error wrapping |

No dependency on `coordinator`, `gwat` or any other GPL/LGPL code. gwat is reached only over
JSON-RPC (`dag_finalize`, `dag_coordinatedState`) by a minimal client built on `net/http`.

---

## Proto contract

The gRPC service is defined in `coordinator/proto/prysm/v1alpha1/coordinator_internal.proto` (Apache 2.0).  
The generated Go code is vendored into `proto/coordpb/` so this module stays self-contained.

Service name on the wire: `ethereum.eth.v1alpha1.CoordinatorInternal`

---

## Related repositories

| Repository | License | Role |
|------------|---------|------|
| `coordinator` | GPL-3.0 | Prysm-based beacon node; exposes `CoordinatorInternal` gRPC |
| `gwat` | LGPL-3.0 | Execution layer; exposes `dag_finalize` JSON-RPC |
| `iguazu-engine` | Apache 2.0 | Engine host that loads gwat as a runtime plugin |
| `iguazu-consensus` | Apache 2.0 | Pure Iguazu consensus library |
| `iguazu-types` | Apache 2.0 | Shared types and interfaces |

---

## Contributing

Iguazu is an [LF Decentralized Trust](https://www.lfdecentralizedtrust.org/) project. See
[CONTRIBUTING.md](CONTRIBUTING.md) for how to contribute, [MAINTAINERS.md](MAINTAINERS.md) for
the maintainers, [SECURITY.md](SECURITY.md) for reporting vulnerabilities and
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for the Code of Conduct.

## License

Apache License 2.0 — see [LICENSE](LICENSE).