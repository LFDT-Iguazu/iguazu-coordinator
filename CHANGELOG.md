# Changelog

Release notes for every version are published with its
[GitHub Release](https://github.com/LFDT-Iguazu/iguazu-coordinator/releases).

## v0.2.0

### Changed

- Requires Go 1.26 (`go 1.26.0`, toolchain `go1.26.11`); modules depending on `iguazu-coordinator` must use Go 1.26 or later.
- Depends on `iguazu-types` v0.4.0 instead of the retracted v0.1.0.
- Dependencies updated: `google.golang.org/grpc` v1.85.0, `google.golang.org/protobuf` v1.36.12, `github.com/sirupsen/logrus` v1.10.2, `github.com/urfave/cli/v2` v2.27.7, `golang.org/x/net` v0.62.0, `golang.org/x/crypto` v0.59.0, `golang.org/x/sys` v0.50.0, `golang.org/x/text` v0.44.0.
- The gRPC connection to the coordinator node is created with `grpc.NewClient` instead of the deprecated `grpc.Dial`; it is still established lazily.

### Fixed

- The finalization loop closes the `StreamNewHeads` stream before it reconnects after an error. Streams left open made the coordinator node block on sending new heads.
- The finalization loop reports a failed `dag_finalize` to the coordinator node through the new `error` field of `SubmitFinalizationResultRequest`, so the node resynchronizes gwat. A result without a last finalized spine is now treated as a failure instead of being ignored.
- The finalization loop skips a head while the coordinator node synchronizes gwat (`GetFinalizationParams` returns `Unavailable`) instead of reconnecting.

### Added

- Validator sync operations passed to `dag_finalize` carry the stake amount of a withdrawal (`ValidatorSync.StakeAmount`), which the execution layer needs after the ValSyncProc fork. The coordinator node sends it in the new `stake_amount_wei` field of `ValidatorSyncOp`; empty bytes mean the amount is not set.
- GitHub Actions: CodeQL static analysis (security and quality queries).
- GitHub Actions: CI (build, vet, test), OpenSSF Scorecard and signed releases with SLSA provenance (see [RELEASE.md](RELEASE.md)); Dependabot.
- Repository files required by LF Decentralized Trust: `RELEASE.md`, `NOTICE`, `ADOPTERS.md`, issue and pull request templates.

## v0.1.0

- Initial release of `iguazu-coordinator` in the LF Decentralized Trust Iguazu project.
