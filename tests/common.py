#!/usr/bin/env python
"""Initialize testing environment"""

# pylint: disable=missing-docstring

import os
import pathlib
import subprocess
from typing import List

# test environment bucket
# No google cloud storage emulator at this point of time [2018-10-31]
# https://cloud.google.com/sdk/gcloud/reference/beta/emulators/
# https://github.com/googleapis/google-cloud-python/issues/4897
# https://github.com/googleapis/google-cloud-python/issues/4840

TEST_GCS_BUCKET = None  # type: str
TEST_GCS_BUCKET_NO_ACCESS = None  # type: str
GCS_FILE_CONTENT = "test file"  # type: str


def gcs_test_setup(tmp_dir_name: str, prefix: str):
    """Create test folders structure to be used in the live test."""
    # yapf: disable
    gcs_file_structure = [
       "{}/{}/d1/d11/f111".format(tmp_dir_name, prefix),
       "{}/{}/d1/d11/f112".format(tmp_dir_name, prefix),
       "{}/{}/d1/f11".format(tmp_dir_name, prefix),
       "{}/{}/d2/f21".format(tmp_dir_name, prefix),
       "{}/{}/d2/f22".format(tmp_dir_name, prefix),
       "{}/{}/d3/d31/d311/f3111".format(tmp_dir_name, prefix),
       "{}/{}/d3/d31/d312/f3131".format(tmp_dir_name, prefix),
       "{}/{}/d3/d31/d312/f3132".format(tmp_dir_name, prefix),
       "{}/{}/d3/d32/f321".format(tmp_dir_name, prefix),
       "{}/{}/play/d1/ff".format(tmp_dir_name, prefix),
       "{}/{}/play/d1/ff".format(tmp_dir_name, prefix),
       "{}/{}/play/d2/ff".format(tmp_dir_name, prefix),
       "{}/{}/play/test1".format(tmp_dir_name, prefix),
       "{}/{}/same_file_different_dir/d1/d11/d111/ff".format(tmp_dir_name,
                                                             prefix),
       "{}/{}/same_file_different_dir/d1/d11/ff".format(tmp_dir_name, prefix),
       "{}/{}/same_file_different_dir/d1/ff".format(tmp_dir_name, prefix),
       "{}/{}/same_file_different_dir/d2/ff".format(tmp_dir_name, prefix)
    ]
    # yapf: enable

    for file in gcs_file_structure:
        path = pathlib.Path(file)
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text(data=GCS_FILE_CONTENT)

    call_gsutil_cp(
        src="{}/{}/".format(tmp_dir_name, prefix),
        dst="gs://{}/".format(TEST_GCS_BUCKET),
        recursive=True)


def gcs_test_teardown(prefix: str):
    """Remove created test folders structure which was used in the live test."""
    cmd = [
        "gsutil", "-m", "rm", "-r", "gs://{}/{}".format(TEST_GCS_BUCKET, prefix)
    ]

    subprocess.check_call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def call_gsutil_ls(path: str, recursive: bool = False) -> List[str]:
    """Simple wrapper around gsutil ls command used to test the gs-wrap."""
    if recursive:
        cmd = ["gsutil", "ls", "-r", path]
    else:
        cmd = ["gsutil", "ls", path]

    proc = subprocess.Popen(
        cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError("{}".format(str(stderr)))

    lines = []  # type: List[str]
    for line in stdout.split('\n'):
        line = line.strip()
        # empty line
        if line == '':
            continue
        # subdirectory matching resolved wildcard *
        if line.endswith('/:'):
            continue
        lines.append(line)

    return lines


def call_gsutil_cp(src: str, dst: str, recursive: bool):
    """Simple wrapper around gsutil cp command used to test the gs-wrap."""
    if recursive:
        cmd = ["gsutil", "-m", "cp", "-r", src, dst]
    else:
        cmd = ["gsutil", "-m", "cp", src, dst]

    subprocess.check_call(
        cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)


def call_gsutil_rm(path: str, recursive: bool = False):
    """Simple wrapper around gsutil rm command used to test the gs-wrap."""
    if recursive:
        cmd = ["gsutil", "-m", "rm", "-r", path]
    else:
        cmd = ["gsutil", "-m", "rm", path]

    subprocess.check_call(
        cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)


def ls_local(path: str) -> List[str]:
    paths = []  # type: List[str]
    for root, dirs, files in os.walk(path):  # pylint: disable=unused-variable
        for file in files:
            paths.append(os.path.join(root, file))

    return paths
