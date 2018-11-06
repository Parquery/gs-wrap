#!/usr/bin/env python
"""Test gs-wrap live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

import os
import pathlib
import shutil
import subprocess
import unittest
import uuid
from typing import List

import google.api_core.exceptions
import temppathlib

import gswrap

# test enviroment bucket
# No google cloud storage emulator at this point of time [31.10.18]
# https://cloud.google.com/sdk/gcloud/reference/beta/emulators/
# https://github.com/googleapis/google-cloud-python/issues/4897
# https://github.com/googleapis/google-cloud-python/issues/4840
TEST_GCS_BUCKET = None  # type: str
TEST_GCS_BUCKET_NO_ACCESS = None  # type: str
GCS_FILE_CONTENT = "test file"  # type: str


def gcs_test_setup(prefix: str):
    """Create test folders structure to be used in the live test."""

    # yapf: disable
    gcs_file_structure = [
       "/tmp/{}/d1/d11/f111".format(prefix),
       "/tmp/{}/d1/d11/f112".format(prefix),
       "/tmp/{}/d1/f11".format(prefix),
       "/tmp/{}/d2/f21".format(prefix),
       "/tmp/{}/d2/f22".format(prefix),
       "/tmp/{}/d3/d31/d311/f3111".format(prefix),
       "/tmp/{}/d3/d31/d312/f3131".format(prefix),
       "/tmp/{}/d3/d31/d312/f3132".format(prefix),
       "/tmp/{}/d3/d32/f321".format(prefix),
       "/tmp/{}/play/d1/ff".format(prefix),
       "/tmp/{}/play/d1/ff".format(prefix),
       "/tmp/{}/play/d2/ff".format(prefix),
       "/tmp/{}/play/test1".format(prefix),
       "/tmp/{}/same_file_different_dir/d1/d11/d111/ff".format(prefix),
       "/tmp/{}/same_file_different_dir/d1/d11/ff".format(prefix),
       "/tmp/{}/same_file_different_dir/d1/ff".format(prefix),
       "/tmp/{}/same_file_different_dir/d2/ff".format(prefix)
    ]
    # yapf: enable

    for file in gcs_file_structure:
        path = pathlib.Path(file)
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text(data=GCS_FILE_CONTENT)

    call_gsutil_cp(
        src="/tmp/{}/".format(prefix),
        dst="gs://{}/".format(TEST_GCS_BUCKET),
        recursive=True)


def gcs_test_teardown(prefix: str):
    """Remove created test folders structure which was used in the live test."""
    cmd = [
        "gsutil", "-m", "rm", "-r", "gs://{}/{}".format(TEST_GCS_BUCKET, prefix)
    ]

    subprocess.check_call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    shutil.rmtree(path="/tmp/{}/".format(prefix))


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


class TestLS(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        gcs_test_setup(prefix=self.bucket_prefix)

    def tearDown(self):
        gcs_test_teardown(prefix=self.bucket_prefix)

    def test_gsutil_ls_non_recursive(self):
        # yapf: disable
        test_cases = [
            "gs://{}".format(TEST_GCS_BUCKET),
            "gs://{}/".format(TEST_GCS_BUCKET),
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix)
        ]
        # yapf: enable
        for test_case in test_cases:
            list_gsutil = call_gsutil_ls(path=test_case)
            list_gcs = self.client.ls(gcs_url=test_case, recursive=False)

            self.assertListEqual(list_gsutil, list_gcs)

    def test_gsutil_ls_non_recursive_check_raises(self):
        # yapf: disable
        test_cases = [
            "gs://{}".format(TEST_GCS_BUCKET_NO_ACCESS),
            "gs://{}".format(TEST_GCS_BUCKET[:-1]),
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix[:-1]),
            "gs://{}/{}/d".format(TEST_GCS_BUCKET, self.bucket_prefix)
        ]
        # yapf: enable
        for test_case in test_cases:
            self.assertRaises(RuntimeError, call_gsutil_ls, path=test_case)
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                self.client.ls,
                gcs_url=test_case,
                recursive=False)

    def test_gsutil_ls_recursive(self):
        # yapf: disable
        test_cases = [
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix)
        ]
        # yapf: enable
        for test_case in test_cases:
            list_gsutil = call_gsutil_ls(path=test_case, recursive=True)
            list_gcs = self.client.ls(gcs_url=test_case, recursive=True)
            # order of 'ls -r' is different
            self.assertListEqual(sorted(list_gsutil), sorted(list_gcs))

    def test_gsutil_ls_recursive_check_raises(self):
        # yapf: disable
        test_cases = [
            "gs://bucket-inexistent",
            "gs://{}".format(TEST_GCS_BUCKET_NO_ACCESS),
            "gs://{}".format(TEST_GCS_BUCKET[:-1]),
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix[:-1]),
            "gs://{}/{}/d".format(TEST_GCS_BUCKET, self.bucket_prefix),
        ]
        # yapf: enable
        for test_case in test_cases:
            self.assertRaises(
                RuntimeError, call_gsutil_ls, path=test_case, recursive=True)
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                self.client.ls,
                gcs_url=test_case,
                recursive=True)


class TestCreateRemove(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        gcs_test_setup(prefix=self.bucket_prefix)

    def tearDown(self):
        gcs_test_teardown(prefix=self.bucket_prefix)

    def test_remove_blob(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            tmp_folder = local_tmpdir.path / str(uuid.uuid4())
            tmp_folder.mkdir()
            tmp_path = tmp_folder / 'file'
            tmp_path.write_text('hello')

            self.client.cp(
                src=local_tmpdir.path.as_posix(),
                dst='gs://{}/{}/'.format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            parent = gswrap._GCSPathlib(
                path=local_tmpdir.path.as_posix()).name_of_parent().as_posix(
                    remove_leading_backslash=True)

            files = self.client.ls(
                gcs_url='gs://{}/{}/{}'.format(TEST_GCS_BUCKET,
                                               self.bucket_prefix, parent),
                recursive=True)

            self.assertEqual(1, len(files), "More or less blobs found.")

            self.client.rm(
                gcs_url='gs://{}/{}/{}'.format(TEST_GCS_BUCKET,
                                               self.bucket_prefix, parent),
                recursive=True)

            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                self.client.ls,
                gcs_url='gs://{}/{}/{}'.format(TEST_GCS_BUCKET,
                                               self.bucket_prefix, parent),
                recursive=True)

    def test_gsutil_vs_gswrap_remove_recursive(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            "gs://{}/{}/d1/d11/f111".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
            "gs://{}/{}/d1/d11".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
        ]
        # yapf: enable

        for test_case in test_cases:
            gcs_test_setup(prefix=self.bucket_prefix)
            self.client.rm(gcs_url=test_case, recursive=True)
            list_gcs = call_gsutil_ls(
                path="gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            gcs_test_setup(prefix=self.bucket_prefix)
            call_gsutil_rm(path=test_case, recursive=True)
            list_gsutil = call_gsutil_ls(
                path="gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            self.assertListEqual(sorted(list_gsutil), sorted(list_gcs))

    def test_gsutil_vs_gswrap_remove_recursive_check_raises(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            "gs://{}/{}/d".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d/".format(TEST_GCS_BUCKET, self.bucket_prefix),

        ]
        # yapf: enable

        for test_case in test_cases:
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                self.client.rm,
                gcs_url=test_case,
                recursive=True)

            self.assertRaises(
                subprocess.CalledProcessError,
                call_gsutil_rm,
                path=test_case,
                recursive=True)

    def test_remove_non_recursive(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_case = "gs://{}/{}/d3/d31/d312/f3131".format(TEST_GCS_BUCKET,
                                                          self.bucket_prefix)
        # yapf: enable

        self.client.rm(gcs_url=test_case, recursive=False)
        list_gcs = call_gsutil_ls(
            path="gs://{}/{}/d3/d31/d312/".format(TEST_GCS_BUCKET,
                                                  self.bucket_prefix),
            recursive=False)

        self.assertListEqual([
            'gs://{}/{}/d3/d31/d312/f3132'.format(TEST_GCS_BUCKET,
                                                  self.bucket_prefix)
        ], list_gcs)

    def test_gsutil_vs_gswrap_remove_non_recursive_check_raises(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            "gs://{}/{}/d1/d11".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d/".format(TEST_GCS_BUCKET,self.bucket_prefix),
        ]
        # yapf: enable
        for test_case in test_cases:
            self.assertRaises(
                ValueError, self.client.rm, gcs_url=test_case, recursive=False)

            self.assertRaises(
                subprocess.CalledProcessError,
                call_gsutil_rm,
                path=test_case,
                recursive=False)


class TestCPRemote(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        gcs_test_setup(prefix=self.bucket_prefix)

    def tearDown(self):
        gcs_test_teardown(prefix=self.bucket_prefix)

    def test_copy_file_to_file_in_same_bucket(self):
        src = 'gs://{}/{}/d1/f11'.format(TEST_GCS_BUCKET, self.bucket_prefix)
        dst = 'gs://{}/{}/ftest'.format(TEST_GCS_BUCKET, self.bucket_prefix)

        self.client.cp(src=src, dst=dst, recursive=False)

        src_blob = self.client._bucket.get_blob(
            blob_name="{}/d1/f11".format(self.bucket_prefix))
        src_text = src_blob.download_as_string()

        dst_blob = self.client._bucket.get_blob(
            blob_name="{}/ftest".format(self.bucket_prefix))
        dst_text = dst_blob.download_as_string()

        self.assertEqual(src_text, dst_text)

    def test_copy_folder_in_same_bucket(self):
        src = 'gs://{}/{}/d1/'.format(TEST_GCS_BUCKET, self.bucket_prefix)
        dst = 'gs://{}/{}/dtest1/'.format(TEST_GCS_BUCKET, self.bucket_prefix)

        self.client.cp(src=src, dst=dst, recursive=True)

        src_list = self.client.ls(gcs_url=src, recursive=True)
        dst_list = self.client.ls(gcs_url=dst, recursive=True)

        for src_file, dst_file in zip(src_list, dst_list):
            src_file = src_file.replace(src, dst + 'd1/')
            self.assertEqual(src_file, dst_file)

    def test_copy_files_in_same_bucket(self):
        src = 'gs://{}/{}/d1/'.format(TEST_GCS_BUCKET, self.bucket_prefix)
        dst = 'gs://{}/{}/dtest1'.format(TEST_GCS_BUCKET, self.bucket_prefix)

        self.client.cp(src=src, dst=dst, recursive=True)

        src_list = self.client.ls(gcs_url=src, recursive=True)
        dst_list = self.client.ls(gcs_url=dst, recursive=True)

        for src_file, dst_file in zip(src_list, dst_list):
            src_file = src_file.replace(src, dst + '/')
            self.assertEqual(src_file, dst_file)

    def test_gsutil_vs_gswrap_copy_recursive(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            ["gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/dtest/".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/dtest/".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/dtest".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/dtest".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d3/d31/d311/f3111".format(TEST_GCS_BUCKET,
                                                   self.bucket_prefix),
             "gs://{}/{}/ftest".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d3/d31/d311/f3111".format(TEST_GCS_BUCKET,
                                                      self.bucket_prefix),
             "gs://{}/{}/ftest/".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/ftest".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/ftest/".format(TEST_GCS_BUCKET, self.bucket_prefix)],
        ]
        # yapf: enable

        gsutil_ls_set = set()
        gcs_ls_set = set()

        ls_path = "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix)

        for test_case in test_cases:
            self.client.cp(src=test_case[0], dst=test_case[1], recursive=True)
            gcs_paths = self.client.ls(gcs_url=ls_path, recursive=True)
            [gcs_ls_set.add(path) for path in gcs_paths]
            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

            call_gsutil_cp(src=test_case[0], dst=test_case[1], recursive=True)
            gsutil_paths = self.client.ls(gcs_url=ls_path, recursive=True)
            [gsutil_ls_set.add(path) for path in gsutil_paths]
            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

            self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

        self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_copy_non_recursive(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            ["gs://{}/{}/d3/d31/d311/f3111".format(TEST_GCS_BUCKET,
                                                      self.bucket_prefix),
             "gs://{}/{}/ftest".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d3/d31/d311/f3111".format(TEST_GCS_BUCKET,
                                                      self.bucket_prefix),
             "gs://{}/{}/ftest/".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/ftest".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/ftest/".format(TEST_GCS_BUCKET, self.bucket_prefix)],
        ]
        # yapf: enable

        gsutil_ls_set = set()
        gcs_ls_set = set()

        ls_path = "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix)

        for test_case in test_cases:
            self.client.cp(src=test_case[0], dst=test_case[1], recursive=False)
            gcs_paths = self.client.ls(gcs_url=ls_path, recursive=True)
            [gcs_ls_set.add(path) for path in gcs_paths]
            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

            call_gsutil_cp(src=test_case[0], dst=test_case[1], recursive=False)
            gsutil_paths = self.client.ls(gcs_url=ls_path, recursive=True)
            [gsutil_ls_set.add(path) for path in gsutil_paths]
            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

            self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

        self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_copy_non_recursive_check_raises(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            ["gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/dtest/".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/dtest/".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/dtest".format(TEST_GCS_BUCKET, self.bucket_prefix)],
            ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
             "gs://{}/{}/dtest".format(TEST_GCS_BUCKET, self.bucket_prefix)],
        ]
        # yapf: enable

        for test_case in test_cases:
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                self.client.cp,
                src=test_case[0],
                dst=test_case[1],
                recursive=False)

            self.assertRaises(
                subprocess.CalledProcessError,
                call_gsutil_cp,
                src=test_case[0],
                dst=test_case[1],
                recursive=False)

    def test_cp_no_clobber(self):
        # yapf: disable
        test_case = [
            "gs://{}/{}/d1/d11/f111".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ,
            "gs://{}/{}/play/d2/ff".format(TEST_GCS_BUCKET, self.bucket_prefix)
        ]
        # yapf: enable

        path_f111 = gswrap._UniformPath(res_loc=test_case[0])
        path_ff = gswrap._UniformPath(res_loc=test_case[1])
        blob_f11 = self.client._bucket.get_blob(
            path_f111.prefix.as_posix(remove_leading_backslash=True))
        blob_ff = self.client._bucket.get_blob(
            path_ff.prefix.as_posix(remove_leading_backslash=True))

        timestamp_f11 = blob_f11.updated
        timestamp_ff = blob_ff.updated

        self.client.cp(src=test_case[0], dst=test_case[1], no_clobber=True)

        blob_f11_not_updated = self.client._bucket.get_blob(
            path_f111.prefix.as_posix(remove_leading_backslash=True))
        blob_ff_not_updated = self.client._bucket.get_blob(
            path_ff.prefix.as_posix(remove_leading_backslash=True))

        timestamp_f11_not_updated = blob_f11_not_updated.updated
        timestamp_ff_not_updated = blob_ff_not_updated.updated

        self.assertEqual(timestamp_f11, timestamp_f11_not_updated)
        self.assertEqual(timestamp_ff, timestamp_ff_not_updated)

    def test_cp_clobber(self):
        # yapf: disable
        test_case = [
            "gs://{}/{}/d1/d11/f111".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ,
            "gs://{}/{}/play/d2/ff".format(TEST_GCS_BUCKET, self.bucket_prefix)
        ]
        # yapf: enable

        path_f111 = gswrap._UniformPath(res_loc=test_case[0])
        path_ff = gswrap._UniformPath(res_loc=test_case[1])
        blob_f11 = self.client._bucket.get_blob(
            path_f111.prefix.as_posix(remove_leading_backslash=True))
        blob_ff = self.client._bucket.get_blob(
            path_ff.prefix.as_posix(remove_leading_backslash=True))

        timestamp_f11 = blob_f11.updated
        timestamp_ff = blob_ff.updated

        self.client.cp(src=test_case[0], dst=test_case[1], no_clobber=False)

        blob_f11_not_updated = self.client._bucket.get_blob(
            path_f111.prefix.as_posix(remove_leading_backslash=True))
        blob_ff_updated = self.client._bucket.get_blob(
            path_ff.prefix.as_posix(remove_leading_backslash=True))

        timestamp_f11_not_updated = blob_f11_not_updated.updated
        timestamp_ff_updated = blob_ff_updated.updated

        self.assertEqual(timestamp_f11, timestamp_f11_not_updated)
        self.assertNotEqual(timestamp_ff, timestamp_ff_updated)


class TestCPUpload(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        gcs_test_setup(prefix=self.bucket_prefix)

    def tearDown(self):
        gcs_test_teardown(prefix=self.bucket_prefix)

    def test_upload_two_files(self):

        with temppathlib.TemporaryDirectory() as local_tmpdir:
            tmp_folder = local_tmpdir.path / 'some-folder'
            tmp_folder.mkdir()
            tmp_path = tmp_folder / str(uuid.uuid4())
            tmp_path.write_text('hello')
            other_folder = local_tmpdir.path / 'another-folder'
            other_folder.mkdir()
            other_file = other_folder / str(uuid.uuid4())
            other_file.write_text('hello')

            self.client.cp(
                src=local_tmpdir.path.as_posix(),
                dst='gs://{}/{}/'.format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            content = self.client._bucket.get_blob(blob_name="{}/{}".format(
                self.bucket_prefix,
                tmp_path.relative_to(local_tmpdir.path.parent)))
            content_other_file = self.client._bucket.get_blob(
                blob_name="{}/{}".format(
                    self.bucket_prefix,
                    other_file.relative_to(local_tmpdir.path.parent)))
            text = content.download_as_string()
            text_other_file = content_other_file.download_as_string()
            self.assertEqual(b'hello', text)
            self.assertEqual(b'hello', text_other_file)

    def test_gsutil_vs_gswrap_upload_recursive(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            local_path_posix = local_file.as_posix()
            another_local_file = local_path / 'another-local-file'
            another_local_file.write_text('hello again')

            # yapf: disable
            test_cases = [
                [local_file.as_posix(), "gs://{}/{}/ftest/".format(
                    TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_file.as_posix(), "gs://{}/{}/ftest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/ftest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/local-file".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/ftest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix(), "gs://{}/{}/dtest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix(), "gs://{}/{}/dtest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix() + '/', "gs://{}/{}/dtest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix() + '/', "gs://{}/{}/dtest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
            ]
            # yapf: enable
            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix)

            for test_case in test_cases:
                self.client.cp(
                    src=test_case[0], dst=test_case[1], recursive=True)
                gcs_paths = self.client.ls(gcs_url=ls_path, recursive=True)
                [gcs_ls_set.add(path) for path in gcs_paths]
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)

                call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=True)
                gsutil_paths = self.client.ls(gcs_url=ls_path, recursive=True)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)

                self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_upload_non_recursive(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            local_path_posix = local_file.as_posix()
            another_local_file = local_path / 'another-local-file'
            another_local_file.write_text('hello again')

            # yapf: disable
            test_cases = [
                [local_file.as_posix(), "gs://{}/{}/ftest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_file.as_posix(), "gs://{}/{}/ftest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/ftest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/local-file".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/ftest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
            ]
            # yapf: enable
            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix)

            for test_case in test_cases:
                self.client.cp(
                    src=test_case[0], dst=test_case[1], recursive=False)
                gcs_paths = self.client.ls(gcs_url=ls_path, recursive=True)
                [gcs_ls_set.add(path) for path in gcs_paths]
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)
                call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=False)
                gsutil_paths = self.client.ls(gcs_url=ls_path, recursive=True)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)

                self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_upload_non_recursive_check_raises(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            another_local_file = local_path / 'another-local-file'
            another_local_file.write_text('hello again')

            # yapf: disable
            test_cases = [
                [local_path.as_posix(), "gs://{}/{}/dtest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix(), "gs://{}/{}/dtest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix() + '/', "gs://{}/{}/dtest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix() + '/', "gs://{}/{}/dtest/".format(
                       TEST_GCS_BUCKET, self.bucket_prefix)],
            ]
            # yapf: enable
            for test_case in test_cases:
                self.assertRaises(
                    ValueError,
                    self.client.cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=False)

                self.assertRaises(
                    subprocess.CalledProcessError,
                    call_gsutil_cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=False)

    def test_upload_no_clobber(self):

        with temppathlib.TemporaryDirectory() as local_tmpdir:
            tmp_path = local_tmpdir.path / str(uuid.uuid4())
            tmp_path.write_text('hello')

            blob_f11 = self.client._bucket.get_blob('{}/d1/f11'.format(
                self.bucket_prefix))
            timestamp_f11 = blob_f11.updated

            self.client.cp(
                src=tmp_path.as_posix(),
                dst='gs://{}/{}/d1/f11'.format(TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                no_clobber=True)

            blob_f11_not_updated = self.client._bucket.get_blob(
                '{}/d1/f11'.format(self.bucket_prefix))

            self.assertEqual(timestamp_f11, blob_f11_not_updated.updated)

    def test_upload_clobber(self):

        with temppathlib.TemporaryDirectory() as local_tmpdir:
            tmp_path = local_tmpdir.path / str(uuid.uuid4())
            tmp_path.write_text('hello')

            blob_f11 = self.client._bucket.get_blob('{}/d1/f11'.format(
                self.bucket_prefix))
            timestamp_f11 = blob_f11.updated

            self.client.cp(
                src=tmp_path.as_posix(),
                dst='gs://{}/{}/d1/f11'.format(TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                no_clobber=False)

            blob_f11_not_updated = self.client._bucket.get_blob(
                '{}/d1/f11'.format(self.bucket_prefix))

            self.assertNotEqual(timestamp_f11, blob_f11_not_updated.updated)


class TestCPDownload(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        gcs_test_setup(prefix=self.bucket_prefix)

    def tearDown(self):
        gcs_test_teardown(prefix=self.bucket_prefix)

    def test_download_one_dir(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            local_path = local_tmpdir.path / 'folder'
            local_path.mkdir()
            self.client.cp(
                src='gs://{}/{}/d1/'.format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
                dst=local_path.as_posix(),
                recursive=True)

            file_pth = local_path / 'd1' / 'f11'
            downloaded_text = file_pth.read_bytes()
            content = self.client._bucket.get_blob(
                blob_name="{}/d1/f11".format(self.bucket_prefix))
            text = content.download_as_string()

            self.assertEqual(text, downloaded_text)

    def test_gsutil_vs_gswrap_download_recursive(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            another_local_file = local_path / 'another-local-file'
            another_local_file.write_text('hello again')
            local_dir = local_path / 'local-dir'
            local_dir.mkdir()
            # yapf: disable
            test_cases = [
                ["gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix)
                    , (local_path / 'uninitialized-file').as_posix()],
                ["gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix)
                    , local_file.as_posix()],
                ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                 local_dir.as_posix()],
                ["gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
                    local_dir.as_posix()],
                ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                 local_dir.as_posix() + '/'],
                ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                 local_dir.as_posix() + '/'],
            ]
            # yapf: enable
            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = tmp_dir.path.as_posix()

            for test_case in test_cases:
                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                self.client.cp(
                    src=test_case[0], dst=test_case[1], recursive=True)

                gcs_paths = ls_local(path=ls_path)
                [gcs_ls_set.add(path) for path in gcs_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=True)
                gsutil_paths = ls_local(path=ls_path)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                self.assertListEqual(gsutil_paths, gcs_paths)

            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_download_recursive_check_raises(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            another_local_file = local_path / 'another-local-file'
            another_local_file.write_text('hello again')
            local_dir = local_path / 'local-dir'
            local_dir.mkdir()

            # yapf: disable
            test_cases = [
                ["gs://{}/{}/d3/d31/d311".format(TEST_GCS_BUCKET,
                                                 self.bucket_prefix),
                 local_file.as_posix()],
                ["gs://{}/{}/d3/d31/d311/".format(TEST_GCS_BUCKET,
                                                  self.bucket_prefix),
                 local_file.as_posix()],
            ]
            # yapf: enable

            for test_case in test_cases:
                self.assertRaises(
                    NotADirectoryError,
                    self.client.cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=True)

                self.assertRaises(
                    subprocess.CalledProcessError,
                    call_gsutil_cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=True)

    def test_gsutil_vs_gswrap_download_non_recursive(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            another_local_file = local_path / 'another-local-file'
            another_local_file.write_text('hello again')
            local_dir = local_path / 'local-dir'
            local_dir.mkdir()

            # yapf: disable
            test_cases = [
                ["gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix)
                    , (local_path / 'uninitialized-file').as_posix()],
                ["gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix)
                    , local_file.as_posix()],
            ]
            # yapf: enable

            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = tmp_dir.path.as_posix()

            for test_case in test_cases:
                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                self.client.cp(
                    src=test_case[0], dst=test_case[1], recursive=False)

                gcs_paths = ls_local(path=ls_path)
                [gcs_ls_set.add(path) for path in gcs_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=False)
                gsutil_paths = ls_local(path=ls_path)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                self.assertListEqual(gsutil_paths, gcs_paths)

            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_download_non_recursive_check_raises(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            another_local_file = local_path / 'another-local-file'
            another_local_file.write_text('hello again')
            local_dir = local_path / 'local-dir'
            local_dir.mkdir()

            # yapf: disable
            test_cases = [
                ["gs://{}/{}/d3/d31/d311".format(TEST_GCS_BUCKET,
                                                 self.bucket_prefix),
                 local_file.as_posix()],  # NotADirectoryError
                ["gs://{}/{}/d3/d31/d311/".format(TEST_GCS_BUCKET,
                                                  self.bucket_prefix),
                 local_file.as_posix()],  # NotADirectoryError
                ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                 local_dir.as_posix()],
                ["gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
                 local_dir.as_posix()],
                ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                 local_dir.as_posix() + '/'],
                ["gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                 local_dir.as_posix() + '/'],
            ]
            # yapf: enable

            for test_case in test_cases:
                self.assertRaises(
                    google.api_core.exceptions.GoogleAPIError,
                    self.client.cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=False)

                self.assertRaises(
                    subprocess.CalledProcessError,
                    call_gsutil_cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=False)

    def test_download_no_clobber(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            local_path = local_tmpdir.path / 'file'
            local_path.write_text("don't overwrite")

            self.client.cp(
                src='gs://{}/{}/d1/f11'.format(TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                dst=local_path.as_posix(),
                no_clobber=True)

            self.assertEqual("don't overwrite", local_path.read_text())

    def test_download_clobber(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            local_path = local_tmpdir.path / 'file'
            local_path.write_text("overwrite file")

            self.client.cp(
                src='gs://{}/{}/d1/f11'.format(TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                dst=local_path.as_posix(),
                no_clobber=False)

            self.assertEqual(GCS_FILE_CONTENT, local_path.read_text())


class TestCPLocal(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=TEST_GCS_BUCKET)

    def test_gsutil_vs_gswrap_local_cp_file(self):  # pylint: disable=invalid-name
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_dir = tmp_dir.path / 'tmp'
            local_dir.mkdir()
            local_file = local_dir / 'local-file'
            local_file.write_text('hello')

            for option in [True, False]:

                dst = local_dir / "dst_{}".format(option)

                self.client.cp(
                    src=local_file.as_posix(),
                    dst=dst.as_posix(),
                    recursive=False)

                self.assertEqual("hello", dst.read_text())

    def test_gsutil_vs_gswrap_local_cp_dir(self):  # pylint: disable=invalid-name
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_dir = tmp_dir.path / 'tmp'
            local_dir.mkdir()

            src_dir = local_dir / "src"
            src_dir.mkdir()
            local_file = src_dir / 'local-file'
            local_file.write_text('hello')

            dst_gsutil = local_dir / "dst_gsutil"
            dst_gswrap = local_dir / "dst_gswrap"

            self.client.cp(
                src=src_dir.as_posix(),
                dst=dst_gswrap.as_posix(),
                recursive=True)

            call_gsutil_cp(
                src=src_dir.as_posix(),
                dst=dst_gsutil.as_posix(),
                recursive=True)

            for gsutil_file, gswrap_file in zip(
                    ls_local(dst_gsutil.as_posix()),
                    ls_local(dst_gswrap.as_posix())):

                self.assertEqual(
                    pathlib.Path(gsutil_file).relative_to(dst_gsutil),
                    pathlib.Path(gswrap_file).relative_to(dst_gswrap))

    def test_gsutil_vs_gswrap_local_cp_check_raises(self):  # pylint: disable=invalid-name
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            local_dir = local_path / "local-dir"
            local_dir.mkdir()
            file1 = local_dir / "file1"
            file1.write_text('hello')
            dst_dir = local_path / 'dst-dir'
            dst_dir.mkdir()

            test_case = [local_dir.as_posix(), dst_dir.as_posix(), False]

            self.assertRaises(
                subprocess.CalledProcessError,
                call_gsutil_cp,
                src=test_case[0],
                dst=test_case[1],
                recursive=test_case[2])

            self.assertRaises(
                ValueError,
                self.client.cp,
                src=test_case[0],
                dst=test_case[1],
                recursive=test_case[2])

    def test_cp_local_no_clobber(self):
        with temppathlib.NamedTemporaryFile() as tmp_file1, \
                temppathlib.NamedTemporaryFile() as tmp_file2:
            tmp_file1.path.write_text("hello")
            tmp_file2.path.write_text("hello there")

            self.client.cp(
                src=tmp_file1.path.as_posix(),
                dst=tmp_file2.path.as_posix(),
                no_clobber=True)

            self.assertEqual("hello there", tmp_file2.path.read_text())

    def test_cp_local_clobber(self):
        with temppathlib.NamedTemporaryFile() as tmp_file1, \
                temppathlib.NamedTemporaryFile() as tmp_file2:
            tmp_file1.path.write_text("hello")
            tmp_file2.path.write_text("hello there")

            self.client.cp(
                src=tmp_file1.path.as_posix(),
                dst=tmp_file2.path.as_posix(),
                no_clobber=False)

            self.assertEqual("hello", tmp_file2.path.read_text())


if __name__ == '__main__':
    unittest.main()
