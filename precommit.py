#!/usr/bin/env python3
"""Runs precommit checks on the repository."""
import argparse
import os
import pathlib
import subprocess
import sys
from typing import List  # pylint: disable=unused-imports


def main() -> int:
    """"
    Main routine
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--overwrite",
        help=
        "Overwrites the unformatted source files with the well-formatted code in place. "
        "If not set, an exception is raised if any of the files do not conform to the style guide.",
        action='store_true')

    args = parser.parse_args()

    overwrite = bool(args.overwrite)

    repo_root = pathlib.Path(__file__).parent

    print("YAPF'ing...")
    if overwrite:
        # yapf: disable
        subprocess.check_call([
            "yapf", "--in-place", "--style=style.yapf", "--recursive",
            "tests", "gswrap", "benchmark", "setup.py", "precommit.py"],
            cwd=str(repo_root))
        # yapf: enable
    else:
        # yapf: disable
        subprocess.check_call([
            "yapf", "--diff", "--style=style.yapf", "--recursive",
            "tests", "gswrap", "benchmark", "setup.py", "precommit.py"],
            cwd=str(repo_root))
        # yapf: enable

    print("Mypy'ing...")
    subprocess.check_call(["mypy", "--strict", "gswrap", "tests", "benchmark"],
                          cwd=str(repo_root))

    print("Isort'ing...")
    isort_files = []  # type: List[str]
    for path in (repo_root / "gswrap").glob("**/*.py"):
        isort_files.append(str(path))
    for path in (repo_root / "tests").glob("**/*.py"):
        isort_files.append(str(path))
    for path in (repo_root / "benchmark").glob("**/*.py"):
        isort_files.append(str(path))

    if overwrite:
        cmd = [
            "isort", "--balanced", "--multi-line", "4", "--line-width", "80",
            "--dont-skip", "__init__.py", "--project", "gswrap"
        ]
        cmd.extend(isort_files)
        subprocess.check_call(cmd)
    else:
        cmd = [
            "isort", "--check-only", "--diff", "--balanced", "--multi-line",
            "4", "--line-width", "80", "--dont-skip", "__init__.py",
            "--project", "gswrap"
        ]
        cmd.extend(isort_files)
        subprocess.check_call(cmd)

    print("Pylint'ing...")
    subprocess.check_call(
        ["pylint", "--rcfile=pylint.rc", "tests", "gswrap", "benchmark"],
        cwd=str(repo_root))

    print("Pydocstyle'ing...")
    subprocess.check_call(["pydocstyle", "gswrap"], cwd=str(repo_root))

    print("Testing...")
    env = os.environ.copy()
    env['ICONTRACT_SLOW'] = 'true'

    subprocess.check_call([
        "coverage", "run", "--source", "gswrap", "-m", "unittest", "discover",
        "tests"
    ],
                          cwd=str(repo_root),
                          env=env)

    subprocess.check_call(["coverage", "report"])

    print("Doctesting...")
    subprocess.check_call(
        [sys.executable, "-m", "doctest",
         str(repo_root / "README.rst")])
    for pth in (repo_root / "gswrap").glob("**/*.py"):
        subprocess.check_call([sys.executable, "-m", "doctest", str(pth)])

    print("pyicontract-lint'ing...")
    for pth in (repo_root / "gswrap").glob("**/*.py"):
        subprocess.check_call(["pyicontract-lint", str(pth)])

    print("Twine'ing...")
    subprocess.check_call([sys.executable, "setup.py", "sdist", "bdist_wheel"],
                          cwd=str(repo_root))
    subprocess.check_call(["twine", "check", "dist/*"], cwd=str(repo_root))

    return 0


if __name__ == "__main__":
    sys.exit(main())
