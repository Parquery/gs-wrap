#!/usr/bin/env python3
"""Benchmark library against other available libraries."""

import argparse
import sys
import warnings
from typing import Any, List

import benchmark

# pylint: disable=missing-docstring


class Args:
    """Represent parsed command-line arguments."""

    def __init__(self, args: Any) -> None:
        """Initialize with arguments parsed with ``argparse``."""
        self.bucket = str(args.bucket)
        self.no_warnings = bool(args.no_warnings)


def parse_args(sys_argv: List[str]) -> Args:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--no_warnings", help="Don't show any warnings", action='store_true')
    parser.add_argument(
        "bucket",
        help="Specify name of a accessible "
        "google cloud storage bucket")
    args = parser.parse_args(sys_argv[1:])

    return Args(args=args)


def _main(args: Args) -> int:
    if args.no_warnings:
        warnings.filterwarnings("ignore")
    test = benchmark.Benchmark(bucket=args.bucket)
    test.run()

    return 0


def main() -> None:
    """Wrap the main routine so that it can be tested."""
    args = parse_args(sys_argv=sys.argv)
    sys.exit(_main(args=args))


if __name__ == '__main__':
    main()
