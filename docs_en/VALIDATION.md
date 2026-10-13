# Validation Record and Limitations

[中文](../docs/VALIDATION.md) | [English overview](../README_en.md)

## Reproduce the Selected Tests

On macOS with Go 1.26+, Python 3, a C compiler, and system SQLite installed:

```sh
python3 scripts/test.py
```

The script sets `GOPROXY=off`, `GOSUMDB=off`, `GOTOOLCHAIN=local`, and `GOWORK=off`, uses disposable Go caches and test state, disables Go telemetry for the test processes through a temporary telemetry directory, and runs only `go test -json -count=1 ./internal/...` in this repository. It does not call external APIs, configure accounts, or start a daemon. History-interface tests bind temporary local Unix sockets; an execution sandbox may prohibit this operation.

Within such a sandbox, the verification scope can be explicitly narrowed:

```sh
python3 scripts/test.py --without-sockets
```

This option excludes only the five top-level `TestHistoryServer*` tests. Protocol encoding and all queue/store/normalization/correlation/activity/filesystem tests still run. This explicitly limits scope; it is not an automatic fallback concealing failed transport checks.

## Verified Scope

On macOS, an explicit `--without-sockets` run produced **63 passing events and zero failures**, including nested subtests. Seven packages have tests; the model package has none.

The five top-level `TestHistoryServer*` tests were not fully verified because the execution environment restricted local socket binding. The original tests are retained in full, and the complete command can be run where binding is permitted. Filtered-out tests are not counted as skipped events.

## Mapping Claims to Test Evidence

| Claim verified | Representative original test |
| --- | --- |
| Duplicate input events do not create another queued task | `TestDuplicateEventAndFIFO` |
| Failures confirmed to precede submission can be retried | `TestCertifiedPreSubmitCanRetry` |
| Uncertain submitted turns are not automatically resubmitted | `TestUncertainNeverResubmits` |
| Uncertain deliveries are not automatically repeated | `TestDeliveryUncertainIsNotRetried` |
| GUI leases affect local task acquisition | `TestExternalGUIRacePolicy` |
| The wrong thread/turn is not accepted as the answer | `TestThreadTurnCorrelation` |
| Epoch append and alias modification share the same transaction outcome | `TestEpochAppendFailureRollsBackAliasCAS` |
| Migration does not fabricate earlier history | `TestSchemaFourMigrationCreatesHonestBaseline` |
| Reaction cleanup does not bombard a failing platform with frequent requests | `TestFeishuDeleteFailureBacksOffWithoutStorm` |
| Private paths reject symbolic links or state with overly broad permissions | `TestRejectsUnsafeAndSymlink` |

The original tests use temporary databases and synthetic fakes. Passing establishes these local contracts; it does not establish online Codex protocol compatibility, platform availability, production health, deployment security, load capacity, or end-to-end exactly-once behavior. This repository makes no claim to have measured performance metrics.
