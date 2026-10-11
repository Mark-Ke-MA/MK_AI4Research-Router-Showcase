#!/usr/bin/env python3
"""Run only this showcase's synthetic tests, offline, using disposable caches."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--without-sockets", action="store_true",
                    help="Explicitly exclude TestHistoryServer* when local socket binding is unavailable.")
args = parser.parse_args()
go = shutil.which("go")
if not go:
    sys.exit("Install Go 1.26 or newer locally before running these tests.")

with tempfile.TemporaryDirectory(prefix="router-showcase-") as temp:
    scratch = Path(temp).resolve()
    telemetry = scratch / "telemetry"
    telemetry.mkdir()
    (telemetry / "mode").write_text("off\n")
    env = os.environ.copy()
    env.update({
        "TMPDIR": str(scratch), "GOCACHE": str(scratch / "cache"),
        "GOPATH": str(scratch / "gopath"), "GOMODCACHE": str(scratch / "modules"),
        "GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local",
        "GOENV": "off", "GOWORK": "off", "GOFLAGS": "",
        "TEST_TELEMETRY_DIR": str(telemetry), "CGO_ENABLED": "1",
    })
    print(subprocess.check_output([go, "version"], env=env, text=True).strip(), flush=True)
    command = [go, "test", "-json", "-count=1"]
    if args.without_sockets:
        print("Explicit scope: TestHistoryServer* excluded; socket transport is not validated.", flush=True)
        command += ["-skip", "^TestHistoryServer"]
    command += ["./internal/..."]
    result = subprocess.run(command,
                            cwd=root, env=env, text=True, capture_output=True)
    passed = failed = skipped = 0
    for line in result.stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            print(line)
            continue
        action = event.get("Action")
        if event.get("Test"):
            passed += action == "pass"
            failed += action == "fail"
            skipped += action == "skip"
        elif action in {"pass", "fail", "skip"}:
            print(f"{action}: {event['Package']}")
        if action == "output" and result.returncode:
            print(event.get("Output", ""), end="")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="")
    print(f"Test events: passed={passed}, failed={failed}, skipped={skipped}")
    sys.exit(result.returncode)
