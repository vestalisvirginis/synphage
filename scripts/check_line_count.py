#!/usr/bin/env python3

import sys

MAX_LINES = 800
files = sys.argv[1:]
failed = []

for f in files:
    try:
        with open(f, encoding="utf-8", errors="ignore") as fh:
            n = sum(1 for _ in fh)
        if n > MAX_LINES:
            failed.append((f, n))
            print(f"ERROR: {f} has {n} lines (max {MAX_LINES})")
    except OSError:
        pass

sys.exit(1 if failed else 0)
