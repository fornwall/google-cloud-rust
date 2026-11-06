# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare one Secret Manager binary with HTTP telemetry enabled and disabled."""

import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "target" / "telemetry-size"
PACKAGE = "telemetry-size-repro"
SWITCH = "--cfg google_cloud_unstable_no_http_telemetry"


def run(*args, env=None, capture=False):
    return subprocess.run(
        args, cwd=ROOT, env=env, check=True, text=True,
        stdout=subprocess.PIPE if capture else None,
    ).stdout


def main():
    # Keep both builds comparable and avoid silently overriding caller flags.
    if os.environ.get("RUSTFLAGS") or os.environ.get("CARGO_ENCODED_RUSTFLAGS"):
        raise SystemExit("Run without RUSTFLAGS or CARGO_ENCODED_RUSTFLAGS")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    versions = run("rustc", "-vV", capture=True) + run("cargo", "bloat", "--version", capture=True)
    (OUTPUT / "toolchain.txt").write_text(versions)
    print(versions, flush=True)
    measurements = {}
    for mode, flags in [("enabled", ""), ("disabled", SWITCH)]:
        target = OUTPUT / mode
        env = dict(os.environ, CARGO_TARGET_DIR=str(target), RUSTFLAGS=flags)
        run("cargo", "build", "--release", "--locked", "-p", PACKAGE, env=env)
        report = run(
            "cargo", "bloat", "--release", "--locked", "-p", PACKAGE,
            "--bin", PACKAGE, "--crates", "-n", "0", "--message-format", "json",
            env=env, capture=True,
        )
        (OUTPUT / f"{mode}-bloat.json").write_text(report)
        sizes = json.loads(report)
        binary = target / "release" / PACKAGE
        stripped = target / "release" / f"{PACKAGE}.stripped"
        run("strip", "--strip-all", "-o", str(stripped), str(binary))
        measurements[mode] = {
            "stripped_bytes": stripped.stat().st_size,
            "text_bytes": sizes["text-section-size"],
        }
    (OUTPUT / "measurements.json").write_text(json.dumps(measurements, indent=2) + "\n")
    print("| Measurement | Telemetry enabled | Telemetry disabled | Reduction |")
    print("| --- | ---: | ---: | ---: |")
    for label, key in [("Stripped executable", "stripped_bytes"), ("`.text` section", "text_bytes")]:
        before, after = (measurements[mode][key] for mode in ["enabled", "disabled"])
        saved = before - after
        print(f"| {label} | {before:,} B | {after:,} B | {saved:,} B ({saved / before:.1%}) |")
    print(f"\nArtifacts: {OUTPUT}")


if __name__ == "__main__":
    main()
