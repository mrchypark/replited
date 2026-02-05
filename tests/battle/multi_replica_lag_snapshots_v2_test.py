#!/usr/bin/env python3
"""Compatibility wrapper for the v2 lag+snapshot battle test.

The plan references this filename. The actual implementation lives in:
  tests/battle/stream_v2_divergence_test.py
"""

from __future__ import annotations

import runpy
from pathlib import Path


def main() -> None:
    target = Path(__file__).with_name("stream_v2_divergence_test.py")
    runpy.run_path(str(target), run_name="__main__")


if __name__ == "__main__":
    main()
