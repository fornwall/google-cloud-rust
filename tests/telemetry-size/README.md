# HTTP telemetry size prototype

This experiment compares the same Secret Manager program with HTTP telemetry
included and compiled out. It builds on PR #31; it is not the proposed public
Cargo feature. Generated clients are unchanged.

The experimental `google_cloud_unstable_no_http_telemetry` configuration disables
the generated client's tracing selection and request recorder lookup, removes
HTTP metric fields and instrumented request paths, and suppresses trace-context
propagation. `.with_tracing()` and `GOOGLE_CLOUD_RUST_LOGGING` cannot re-enable
these paths in this build. Ordinary logging in dependencies is unaffected.

OpenTelemetry dependencies and generated tracing types are still compiled; some
of their code remains linked. The measurements quantify this prototype's
reduction, not the maximum possible saving from a complete opt-out. A public
opt-out must also handle dependency selection, generated feature forwarding,
and gRPC. This prototype rejects builds that combine the switch with gRPC.

From the repository root, on Linux with Python 3 and GNU binutils installed:

```sh
cargo install cargo-bloat --version 0.12.1 --locked
python3 tests/telemetry-size/measure.py
```

Both builds use the checked-in `Cargo.lock`, the same Rust toolchain, and the
standard release profile. The only build flag difference is the experimental
configuration. The script compares stripped executable bytes and `.text` bytes,
and writes both binaries, cargo-bloat JSON, and the toolchain version under
`target/telemetry-size/`. It does not execute the program or require credentials.
Do not compare these numbers to the earlier crates.io example: that build used a
different dependency lockfile.

The program being measured is [src/main.rs](src/main.rs). Its integration tests
use local HTTP servers and cover credentials, retrying a transient error,
response decoding, permanent errors, and trace propagation through both the
generated client and the raw HTTP request builder. They explicitly request
tracing so that disabling telemetry must override the runtime configuration.

```sh
cargo test --locked -p telemetry-size-repro
RUSTFLAGS='--cfg google_cloud_unstable_no_http_telemetry' \
  cargo test --locked -p telemetry-size-repro
```

Measured on 2026-11-06 with Rust 1.99.1, cargo-bloat 0.12.1, and
`x86_64-unknown-linux-gnu`:

| Measurement | Telemetry enabled | Prototype disabled | Reduction |
| --- | ---: | ---: | ---: |
| Stripped executable | 9,925,192 B | 9,498,640 B | 426,552 B (4.3%) |
| `.text` section | 7,203,350 B | 6,858,086 B | 345,264 B (4.8%) |

This saves about 417 KiB of the stripped executable. The earlier 11.4 KiB
estimate counted only symbols attributed directly to OpenTelemetry crates and
missed instrumentation compiled into the SDK itself. Exact results can vary
with the toolchain, linker, target, and dependency lockfile.
