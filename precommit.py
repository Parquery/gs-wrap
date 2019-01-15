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
        subprocess.check_call([
            "yapf", "--in-place", "--style=style.yapf", "--recursive", "tests",
            "gswrap", "benchmark", "setup.py", "precommit.py"
        ],
                              cwd=repo_root.as_posix())
    else:
        subprocess.check_call([
            "yapf", "--diff", "--style=style.yapf", "--recursive", "tests",
            "gswrap", "benchmark", "setup.py", "precommit.py"
        ],
                              cwd=repo_root.as_posix())

    print("Mypy'ing...")
    subprocess.check_call(["mypy", "gswrap", "tests", "benchmark"],
                          cwd=repo_root.as_posix())

    print("Isort'ing...")
    isort_files = []  # type: List[str]
    for path in (repo_root / "gswrap").glob("**/*.py"):
        isort_files.append(path.as_posix())
    for path in (repo_root / "tests").glob("**/*.py"):
        isort_files.append(path.as_posix())
    for path in (repo_root / "benchmark").glob("**/*.py"):
        isort_files.append(path.as_posix())

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
        cwd=repo_root.as_posix())

    print("Pydocstyle'ing...")
    subprocess.check_call(["pydocstyle", "gswrap"], cwd=repo_root.as_posix())

    print("Testing...")
    env = os.environ.copy()
    env['ICONTRACT_SLOW'] = 'true'

    subprocess.check_call([
        "coverage", "run", "--source", "gswrap", "-m", "unittest", "discover",
        "tests"
    ],
                          cwd=repo_root.as_posix(),
                          env=env)

    subprocess.check_call(["coverage", "report"])

    print("Doctesting...")
    subprocess.check_call(
        ["python3", "-m", "doctest", (repo_root / "README.rst").as_posix()])
    for pth in (repo_root / "gswrap").glob("**/*.py"):
        subprocess.check_call(["python3", "-m", "doctest", pth.as_posix()])

    print("pyicontract-lint'ing...")
    for pth in (repo_root / "gswrap").glob("**/*.py"):
        subprocess.check_call(["pyicontract-lint", pth.as_posix()])

    print("Twine'ing...")
    subprocess.check_call(["python3", "setup.py", "sdist", "bdist_wheel"],
                          cwd=repo_root.as_posix())
    subprocess.check_call(["twine", "check", "dist/*"],
                          cwd=repo_root.as_posix())

    return 0


if __name__ == "__main__":
    sys.exit(main())
