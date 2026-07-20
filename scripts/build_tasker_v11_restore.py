#!/usr/bin/env python3
"""Build V11 with a one-minute last-SMS reconciliation guard."""

from __future__ import annotations

import argparse
from pathlib import Path

from build_tasker_v10_restore import (
    TOKEN_PLACEHOLDER,
    build_restore,
    extract_token,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("template_output", type=Path)
    parser.add_argument("--private-source", type=Path)
    parser.add_argument("--private-output", type=Path)
    args = parser.parse_args()

    build_restore(
        args.source,
        args.template_output,
        transport_version=11,
        include_reconciler=True,
    )
    if bool(args.private_source) != bool(args.private_output):
        parser.error("--private-source and --private-output must be used together")
    if args.private_source and args.private_output:
        token = extract_token(args.private_source)
        private_text = args.template_output.read_text(encoding="utf-8").replace(
            TOKEN_PLACEHOLDER,
            token,
        )
        if TOKEN_PLACEHOLDER in private_text:
            raise ValueError("Tasker token placeholder was not fully replaced")
        args.private_output.write_text(private_text, encoding="utf-8")


if __name__ == "__main__":
    main()
