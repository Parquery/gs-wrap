#!/usr/bin/env python
"""Test gs-wrap live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

import concurrent.futures
import os
import pathlib
import shutil
import subprocess
import unittest
import uuid
import warnings
from typing import List

import google.api_core.exceptions
import temppathlib

import gswrap

# TODO(snaji): fixme  pylint: disable=fixme
# test enviroment bucket
# No google cloud storage emulator at this point of time [31.10.18]
# https://cloud.google.com/sdk/gcloud/reference/beta/emulators/
# https://github.com/googleapis/google-cloud-python/issues/4897
# https://github.com/googleapis/google-cloud-python/issues/4840
TEST_GCS_BUCKET = "parquery-sandbox"  # type: str
TEST_GCS_BUCKET_NO_ACCESS = "parquery-data"  # type: str
NO_WARNINGS = True  # type: bool
GCS_FILE_CONTENT = "test file"  # type: str


def gcs_test_setup(prefix: str):
    """Create test folders structure to be used in the live test."""

    # yapf: disable
    gcs_structure = [
        "gs://{}/{}/d1/d11/f111".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/d1/d11/f112".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/d2/f21".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/d2/f22".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/d3/d31/d311/f3111".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/d3/d31/d312/f3131".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/d3/d31/d312/f3132".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/d3/d32/f321".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/play/d1/ff".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/play/d1:/ff".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/play/d2/ff".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/play/test1".format(TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/same_file_different_dir/d1/d11/d111/ff".format(
            TEST_GCS_BUCKET,prefix),
        "gs://{}/{}/same_file_different_dir/d1/d11/ff".format(
            TEST_GCS_BUCKET,prefix),
        "gs://{}/{}/same_file_different_dir/d1/ff".format(
            TEST_GCS_BUCKET,prefix),
        "gs://{}/{}/same_file_different_dir/d2/ff".format(
            TEST_GCS_BUCKET, prefix)
    ]

    # yapf: enable
    futures = []  # type: List[concurrent.futures.Future]
    with temppathlib.NamedTemporaryFile() as tmp_file:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            tmp_file.path.write_text(GCS_FILE_CONTENT)
            for file in gcs_structure:

                cmd = [
                    'gsutil', '-q', 'cp', '-n',
                    tmp_file.path.as_posix(), file
                ]

                setup_thread = executor.submit(
                    subprocess.check_call, cmd, stdin=subprocess.PIPE)
                futures.append(setup_thread)


def gcs_test_teardown(prefix: str):
    """Remove created test folders structure which was used in the live test."""
    cmd = [
        "gsutil", "-m", "rm", "-r", "gs://{}/{}".format(TEST_GCS_BUCKET, prefix)
    ]

    subprocess.check_call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def call_gsutil_ls(path: str, recursive: bool = False) -> List[str]:
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


def call_gcs_client_ls(client: gswrap.Client,
                       path: str,
                       recursive: bool = False) -> List[str]:

    list_ls = client.ls(gcs_url=path, recursive=recursive)

    return list_ls


class TestLS(unittest.TestCase):
    def setUp(self):
        # pylint: disable=fixme
        # TODO(snaji): remove warning filters
        if NO_WARNINGS:
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                message=
                "Your application has authenticated using end user credentials "
                "from Google Cloud SDK.*")
            warnings.filterwarnings(
                "ignore",
                category=ResourceWarning,
                message="unclosed.*<ssl.SSLSocket.*>")

        self.client = gswrap.Client(bucket_name=TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        gcs_test_setup(prefix=self.bucket_prefix)

    def tearDown(self):
        gcs_test_teardown(prefix=self.bucket_prefix)

    def test_gsutil_ls_non_recursive(self):
        test_cases = [
            "gs://{}".format(TEST_GCS_BUCKET),
            "gs://{}/".format(TEST_GCS_BUCKET), "gs://{}/{}".format(
                TEST_GCS_BUCKET, self.bucket_prefix), "gs://{}/{}/".format(
                    TEST_GCS_BUCKET,
                    self.bucket_prefix), "gs://{}/{}/d1".format(
                        TEST_GCS_BUCKET,
                        self.bucket_prefix), "gs://{}/{}/d1/".format(
                            TEST_GCS_BUCKET, self.bucket_prefix)
        ]

        for test_case in test_cases:
            list_gsutil = call_gsutil_ls(path=test_case)
            list_gcs = call_gcs_client_ls(client=self.client, path=test_case)

            self.assertListEqual(list_gsutil, list_gcs)

    def test_gsutil_ls_non_recursive_check_raises(self):

        test_cases = [
            "gs://{}".format(TEST_GCS_BUCKET_NO_ACCESS),
            "gs://{}".format(TEST_GCS_BUCKET[:-1]), "gs://{}/{}".format(
                TEST_GCS_BUCKET,
                self.bucket_prefix[:-1]), "gs://{}/{}/d".format(
                    TEST_GCS_BUCKET, self.bucket_prefix)
        ]
        for test_case in test_cases:
            self.assertRaises(RuntimeError, call_gsutil_ls, path=test_case)
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                call_gcs_client_ls,
                client=self.client,
                path=test_case)

    def test_gsutil_ls_recursive(self):
        test_cases = [
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1".format(TEST_GCS_BUCKET,
                                   self.bucket_prefix), "gs://{}/{}/d1/".format(
                                       TEST_GCS_BUCKET, self.bucket_prefix)
        ]

        for test_case in test_cases:
            list_gsutil = call_gsutil_ls(path=test_case, recursive=True)
            list_gcs = call_gcs_client_ls(
                client=self.client, path=test_case, recursive=True)
            # order of 'ls -r' is different
            self.assertListEqual(sorted(list_gsutil), sorted(list_gcs))

    def test_gsutil_ls_recursive_check_raises(self):
        test_cases = [
            "gs://bucket-inexistent",
            "gs://{}".format(TEST_GCS_BUCKET_NO_ACCESS),
            "gs://{}".format(TEST_GCS_BUCKET[:-1]),
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix[:-1]),
            "gs://{}/{}/d".format(TEST_GCS_BUCKET, self.bucket_prefix),
        ]

        for test_case in test_cases:
            self.assertRaises(
                RuntimeError, call_gsutil_ls, path=test_case, recursive=True)
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                call_gcs_client_ls,
                client=self.client,
                path=test_case,
                recursive=True)


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


class TestCPRemote(unittest.TestCase):
    def setUp(self):
        if NO_WARNINGS:
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                message=
                "Your application has authenticated using end user credentials "
                "from Google Cloud SDK.*")
            warnings.filterwarnings(
                "ignore",
                category=ResourceWarning,
                message="unclosed.*<ssl.SSLSocket.*>")

        self.client = gswrap.Client(bucket_name=TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        gcs_test_setup(prefix=self.bucket_prefix)

    def tearDown(self):
        gcs_test_teardown(prefix=self.bucket_prefix)

    def test_copy_folder_in_same_bucket(self):
        src = 'gs://{}/{}/d1/'.format(TEST_GCS_BUCKET, self.bucket_prefix)
        dst = 'gs://{}/{}/dtest1/'.format(TEST_GCS_BUCKET, self.bucket_prefix)

        self.client.cp(src=src, dst=dst, recursive=True)

        src_list = self.client.ls(gcs_url=src, recursive=True)
        dst_list = self.client.ls(gcs_url=dst, recursive=True)

        for src_file, dst_file in zip(src_list, dst_list):
            src_file = src_file.replace(src, dst + 'd1/')
            self.assertEqual(src_file, dst_file)

        self.client.rm(gcs_url=dst, recursive=True)

        src2 = 'gs://{}/{}/d2'.format(TEST_GCS_BUCKET, self.bucket_prefix)
        dst2 = 'gs://{}/{}/dtest2/'.format(TEST_GCS_BUCKET, self.bucket_prefix)

        self.client.cp(src=src2, dst=dst2, recursive=True)

        src_list2 = self.client.ls(gcs_url=src2, recursive=True)
        dst_list2 = self.client.ls(gcs_url=dst2, recursive=True)

        for src_file, dst_file in zip(src_list2, dst_list2):
            src_file = src_file.replace(src2, dst2 + 'd2')
            self.assertEqual(src_file, dst_file)

        self.client.rm(gcs_url=dst2, recursive=True)

    def test_copy_files_in_same_bucket(self):
        src = 'gs://{}/{}/d1/'.format(TEST_GCS_BUCKET, self.bucket_prefix)
        dst = 'gs://{}/{}/dtest1'.format(TEST_GCS_BUCKET, self.bucket_prefix)

        self.client.cp(src=src, dst=dst, recursive=True)

        src_list = self.client.ls(gcs_url=src, recursive=True)
        dst_list = self.client.ls(gcs_url=dst, recursive=True)

        for src_file, dst_file in zip(src_list, dst_list):
            src_file = src_file.replace(src, dst + '/')
            self.assertEqual(src_file, dst_file)

        self.client.rm(gcs_url=dst, recursive=True)

        src2 = 'gs://{}/{}/d2'.format(TEST_GCS_BUCKET, self.bucket_prefix)
        dst2 = 'gs://{}/{}/dtest2'.format(TEST_GCS_BUCKET, self.bucket_prefix)

        self.client.cp(src=src2, dst=dst2, recursive=True)

        src_list2 = self.client.ls(gcs_url=src2, recursive=True)
        dst_list2 = self.client.ls(gcs_url=dst2, recursive=True)

        for src_file, dst_file in zip(src_list2, dst_list2):
            src_file = src_file.replace(src2, dst2)
            self.assertEqual(src_file, dst_file)

        self.client.rm(gcs_url=dst2, recursive=True)

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

            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

            self.assertRaises(
                subprocess.CalledProcessError,
                call_gsutil_cp,
                src=test_case[0],
                dst=test_case[1],
                recursive=False)
            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

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
        if NO_WARNINGS:
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                message=
                "Your application has authenticated using end user credentials "
                "from Google Cloud SDK.*")
            warnings.filterwarnings(
                "ignore",
                category=ResourceWarning,
                message="unclosed.*<ssl.SSLSocket.*>")

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

            content.delete()
            content_other_file.delete()

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
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)

                self.assertRaises(
                    subprocess.CalledProcessError,
                    call_gsutil_cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=False)
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)

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


def ls_local(path: str) -> List[str]:
    paths = []  # type: List[str]
    for root, dirs, files in os.walk(path):  # pylint: disable=unused-variable
        for file in files:
            paths.append(os.path.join(root, file))

    return paths


class TestCPDownload(unittest.TestCase):
    def setUp(self):
        if NO_WARNINGS:
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                message=
                "Your application has authenticated using end user credentials "
                "from Google Cloud SDK.*")
            warnings.filterwarnings(
                "ignore",
                category=ResourceWarning,
                message="unclosed.*<ssl.SSLSocket.*>")

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
                ["gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET,self.bucket_prefix),
                    local_file.as_posix()],
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
                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                self.assertRaises(
                    NotADirectoryError,
                    self.client.cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=True)
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                self.assertRaises(
                    subprocess.CalledProcessError,
                    call_gsutil_cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=True)
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

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
                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                self.assertRaises(
                    google.api_core.exceptions.GoogleAPIError,
                    self.client.cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=False)

                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                self.assertRaises(
                    subprocess.CalledProcessError,
                    call_gsutil_cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=False)

                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

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
        if NO_WARNINGS:
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                message=
                "Your application has authenticated using end user credentials "
                "from Google Cloud SDK.*")
            warnings.filterwarnings(
                "ignore",
                category=ResourceWarning,
                message="unclosed.*<ssl.SSLSocket.*>")

        self.client = gswrap.Client(bucket_name=TEST_GCS_BUCKET)

    def test_gsutil_vs_gswrap_local_cp(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            local_dir = local_path / "local-dir"
            local_dir.mkdir()
            file1 = local_dir / "file1"
            file1.write_text('hello')
            file2 = local_dir / "file2"
            file2.write_text('hi there')
            dst_dir = local_path / 'dst-dir'
            dst_dir.mkdir()

            # yapf: disable
            test_cases = [
                [local_file.as_posix(), (local_path / 'another-local-file'
                                         ).as_posix(), True],
                [local_dir.as_posix(), dst_dir.as_posix(), True],
                [local_file.as_posix(), (local_path / 'another-local-file'
                                         ).as_posix(), False]
            ]
            # yapf: enable
            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = tmp_dir.path.as_posix()

            for test_case in test_cases:
                call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=test_case[2])
                gsutil_paths = ls_local(path=ls_path)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)
                elif pathlib.Path(test_case[1]).exists():
                    os.remove(test_case[1])

                if pathlib.Path(test_case[0]).is_dir() and pathlib.Path(
                        test_case[1]).is_dir():
                    shutil.rmtree(test_case[1])
                self.client.cp(
                    src=test_case[0], dst=test_case[1], recursive=test_case[2])

                gcs_paths = ls_local(path=ls_path)
                [gcs_ls_set.add(path) for path in gcs_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)
                elif pathlib.Path(test_case[1]).exists():
                    os.remove(test_case[1])

                self.assertListEqual(gsutil_paths, gcs_paths)

                self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_local_cp_check_raises(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            local_dir = local_path / "local-dir"
            local_dir.mkdir()
            file1 = local_dir / "file1"
            file1.write_text('hello')
            file2 = local_dir / "file2"
            file2.write_text('hi there')
            dst_dir = local_path / 'dst-dir'
            dst_dir.mkdir()

            test_cases = [[local_dir.as_posix(), dst_dir.as_posix(), False]]

            for test_case in test_cases:
                self.assertRaises(
                    subprocess.CalledProcessError,
                    call_gsutil_cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=test_case[2])
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)
                elif pathlib.Path(test_case[1]).exists():
                    os.remove(test_case[1])

                if pathlib.Path(test_case[0]).is_dir() and pathlib.Path(
                        test_case[1]).is_dir():
                    shutil.rmtree(test_case[1])
                self.assertRaises(
                    ValueError,
                    self.client.cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=test_case[2])

                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)
                elif pathlib.Path(test_case[1]).exists():
                    os.remove(test_case[1])

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


class TestCreateRemove(unittest.TestCase):
    def setUp(self):
        if NO_WARNINGS:
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                message=
                "Your application has authenticated using end user credentials "
                "from Google Cloud SDK.*")
            warnings.filterwarnings(
                "ignore",
                category=ResourceWarning,
                message="unclosed.*<ssl.SSLSocket.*>")

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

            blobs = self.client.ls(
                gcs_url='gs://{}/{}/{}'.format(TEST_GCS_BUCKET,
                                               self.bucket_prefix, parent),
                recursive=True)

            self.assertEqual(1, len(blobs), "More or less blobs found.")

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
            gcs_test_setup(prefix=self.bucket_prefix)
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                self.client.rm,
                gcs_url=test_case,
                recursive=True)

            gcs_test_setup(prefix=self.bucket_prefix)
            self.assertRaises(
                subprocess.CalledProcessError,
                call_gsutil_rm,
                path=test_case,
                recursive=True)

    def test_gsutil_vs_gswrap_remove_non_recursive(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = ["gs://{}/{}/d1/d11/f111".format(TEST_GCS_BUCKET,
                                                      self.bucket_prefix)]
        # yapf: enable
        for test_case in test_cases:
            gcs_test_setup(prefix=self.bucket_prefix)
            self.client.rm(gcs_url=test_case, recursive=False)
            list_gcs = call_gsutil_ls(
                path="gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            gcs_test_setup(prefix=self.bucket_prefix)
            call_gsutil_rm(path=test_case, recursive=False)
            list_gsutil = call_gsutil_ls(
                path="gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            self.assertListEqual(sorted(list_gsutil), sorted(list_gcs))

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
            gcs_test_setup(prefix=self.bucket_prefix)
            self.assertRaises(
                ValueError, self.client.rm, gcs_url=test_case, recursive=False)

            gcs_test_setup(prefix=self.bucket_prefix)
            self.assertRaises(
                subprocess.CalledProcessError,
                call_gsutil_rm,
                path=test_case,
                recursive=False)


if __name__ == '__main__':
    unittest.main()
