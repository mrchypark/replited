#!/usr/bin/env python3
"""Wrapper for sidecar retry scenario."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    script = Path(__file__).with_name("sidecar_restore_retry_recover_test.py")
    result = subprocess.run(
        [sys.executable, str(script), "--scenario", "retry"],
        check=False,
    )
    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
