# Changelog

Release notes for every version are published with its
[GitHub Release](https://github.com/LFDT-Iguazu/iguazu-coordinator/releases).

## Unreleased

### Fixed

- The finalization loop closes the `StreamNewHeads` stream before it reconnects after an error. Streams left open made the coordinator node block on sending new heads.
- The finalization loop reports a failed `dag_finalize` to the coordinator node through the new `error` field of `SubmitFinalizationResultRequest`, so the node resynchronizes gwat. A result without a last finalized spine is now treated as a failure instead of being ignored.
- The finalization loop skips a head while the coordinator node synchronizes gwat (`GetFinalizationParams` returns `Unavailable`) instead of reconnecting.

### Added

- GitHub Actions: CodeQL static analysis (security and quality queries).
- GitHub Actions: CI (build, vet, test), OpenSSF Scorecard and signed releases with SLSA provenance (see [RELEASE.md](RELEASE.md)); Dependabot.
- Repository files required by LF Decentralized Trust: `RELEASE.md`, `NOTICE`, `ADOPTERS.md`, issue and pull request templates.

## v0.1.0

- Initial release of `iguazu-coordinator` in the LF Decentralized Trust Iguazu project.
