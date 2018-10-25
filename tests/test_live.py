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
import time
import unittest
import uuid
import warnings
from typing import List

import google.api_core.exceptions
import temppathlib

import gswrap

# test enviroment bucket
TEST_GCS_BUCKET = None  # type: str
TEST_GCS_BUCKET_NO_ACCESS = None  # type: str
NO_WARNINGS = True  # type: bool
VERBOSE = False  # type: bool
GCS_FILE_CONTENT = "test file"  # type: str


def gcs_test_setup(prefix: str):
    gcs_structure = [
        "gs://{}/{}/d1/d11/f111".format(
            TEST_GCS_BUCKET, prefix), "gs://{}/{}/d1/d11/f112".format(
                TEST_GCS_BUCKET, prefix), "gs://{}/{}/d1/f11".format(
                    TEST_GCS_BUCKET, prefix), "gs://{}/{}/d2/f21".format(
                        TEST_GCS_BUCKET, prefix), "gs://{}/{}/d2/f22".format(
                            TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/d3/d31/d311/f3111".format(
            TEST_GCS_BUCKET, prefix), "gs://{}/{}/d3/d31/d312/f3131".format(
                TEST_GCS_BUCKET, prefix), "gs://{}/{}/d3/d31/d312/f3132".format(
                    TEST_GCS_BUCKET, prefix), "gs://{}/{}/d3/d32/f321".format(
                        TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/play/d1/ff".format(
            TEST_GCS_BUCKET, prefix), "gs://{}/{}/play/d1:/ff".format(
                TEST_GCS_BUCKET, prefix), "gs://{}/{}/play/d2/ff".format(
                    TEST_GCS_BUCKET, prefix), "gs://{}/{}/play/test1".format(
                        TEST_GCS_BUCKET, prefix),
        "gs://{}/{}/same_file_different_dir/d1/d11/d111/ff".format(
            TEST_GCS_BUCKET,
            prefix), "gs://{}/{}/same_file_different_dir/d1/d11/ff".format(
                TEST_GCS_BUCKET,
                prefix), "gs://{}/{}/same_file_different_dir/d1/ff".format(
                    TEST_GCS_BUCKET,
                    prefix), "gs://{}/{}/same_file_different_dir/d2/ff".format(
                        TEST_GCS_BUCKET, prefix)
    ]
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
                    subprocess_single_cmd_execution, cmd=cmd, file=file)
                futures.append(setup_thread)


def subprocess_single_cmd_execution(cmd: List[str], file: str):
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("Failed to write to the object: {}".format(file))


def gcs_test_teardown(prefix: str):
    cmd = [
        "gsutil", "-m", "rm", "-r", "gs://{}/{}".format(TEST_GCS_BUCKET, prefix)
    ]

    proc = subprocess.Popen(
        cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    proc.communicate()


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
        print(stderr)

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

    try:
        list_ls = client.ls(gcs_url=path, recursive=recursive)
    except google.api_core.exceptions.GoogleAPIError as err:
        print(err)
        return []

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
            # no wildcards
            "gs://bucket-inexistent",
            "gs://{}".format(TEST_GCS_BUCKET_NO_ACCESS),
            "gs://{}".format(TEST_GCS_BUCKET[:-1]),
            "gs://{}".format(TEST_GCS_BUCKET),
            "gs://{}/".format(TEST_GCS_BUCKET),
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix[:-1]),
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix)
        ]

        for i, test_case in enumerate(test_cases):
            print("{}/{}".format(i + 1, len(test_cases)))
            start_gsutil = time.time()
            list_gsutil = call_gsutil_ls(path=test_case)
            end_gsutil = time.time()
            start_gcs = time.time()
            list_gcs = call_gcs_client_ls(client=self.client, path=test_case)
            end_gcs = time.time()
            if VERBOSE:
                print('Testcase: {}, Time: gsutil: {}, gcs: {}'.format(
                    test_case, end_gsutil - start_gsutil, end_gcs - start_gcs))
                print('Gsutil: ', list_gsutil)
                print('GCS:    ', list_gcs)
                print('#########################')

            self.assertListEqual(list_gsutil, list_gcs)

    def test_gsutil_ls_recursive(self):
        test_cases = [
            # no wildcards
            "gs://bucket-inexistent",
            "gs://{}".format(TEST_GCS_BUCKET_NO_ACCESS),
            "gs://{}".format(TEST_GCS_BUCKET[:-1]),
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix[:-1]),
            "gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
            "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix)
        ]

        for i, test_case in enumerate(test_cases):
            print("{}/{}".format(i + 1, len(test_cases)))
            start_gsutil = time.time()
            list_gsutil = call_gsutil_ls(path=test_case, recursive=True)
            end_gsutil = time.time()
            start_gcs = time.time()
            list_gcs = call_gcs_client_ls(
                client=self.client, path=test_case, recursive=True)
            end_gcs = time.time()
            if VERBOSE:
                print('Testcase: {}, Time: gsutil: {}, gcs: {}'.format(
                    test_case, end_gsutil - start_gsutil, end_gcs - start_gcs))
                print('Gsutil: ', list_gsutil)
                print('GCS:    ', list_gcs)
                print('#########################')

            # order of 'ls -r' is different
            self.assertListEqual(sorted(list_gsutil), sorted(list_gcs))


def call_gsutil_cp(src: str, dst: str, recursive: bool):
    if recursive:
        cmd = ["gsutil", "-m", "cp", "-r", src, dst]
    else:
        cmd = ["gsutil", "-m", "cp", src, dst]

    proc = subprocess.Popen(
        cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    proc.communicate()


def call_gsutil_rm(path: str, recursive: bool = False):
    if recursive:
        cmd = ["gsutil", "-m", "rm", "-r", path]
    else:
        cmd = ["gsutil", "-m", "rm", path]

    proc = subprocess.Popen(
        cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    proc.communicate()


def call_gcs_client_cp(client: gswrap.Client, src: str, dst: str,
                       recursive: bool):

    try:
        client.cp(src=src, dst=dst, recursive=recursive)
    except google.api_core.exceptions.GoogleAPIError as err:
        print(err)
    except ValueError as valerr:
        print(valerr)


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
        test_cases = [
            [
                "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/dtest/".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/dtest/".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/dtest".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/dtest".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d3/d31/d311/f3111".format(TEST_GCS_BUCKET,
                                                      self.bucket_prefix),
                "gs://{}/{}/ftest".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d3/d31/d311/f3111".format(TEST_GCS_BUCKET,
                                                      self.bucket_prefix),
                "gs://{}/{}/ftest/".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/ftest".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/ftest/".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
        ]

        gsutil_ls_set = set()
        gcs_ls_set = set()

        ls_path = "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix)

        copy_time_gsutil = 0
        copy_time_gcs = 0
        for i, test_case in enumerate(test_cases):
            print("{}/{} : Testcase: {}".format(i + 1, len(test_cases),
                                                test_case))
            if VERBOSE:
                print("gcs start...")
            start = time.time()
            call_gcs_client_cp(
                client=self.client,
                src=test_case[0],
                dst=test_case[1],
                recursive=True)
            end = time.time()
            copy_time_gcs += end - start
            gcs_paths = self.client.ls(gcs_url=ls_path, recursive=True)
            [gcs_ls_set.add(path) for path in gcs_paths]
            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

            if VERBOSE:
                print("gsutil start...")
            start = time.time()
            call_gsutil_cp(src=test_case[0], dst=test_case[1], recursive=True)
            end = time.time()
            copy_time_gsutil += end - start
            gsutil_paths = self.client.ls(gcs_url=ls_path, recursive=True)
            [gsutil_ls_set.add(path) for path in gsutil_paths]
            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

            self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

        if VERBOSE:
            print("time gsutil: {}, gcs: {}".format(copy_time_gsutil,
                                                    copy_time_gcs))
        self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_copy_non_recursive(self):  # pylint: disable=invalid-name
        test_cases = [
            [
                "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/dtest/".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/dtest/".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/dtest".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/dtest".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d3/d31/d311/f3111".format(TEST_GCS_BUCKET,
                                                      self.bucket_prefix),
                "gs://{}/{}/ftest".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d3/d31/d311/f3111".format(TEST_GCS_BUCKET,
                                                      self.bucket_prefix),
                "gs://{}/{}/ftest/".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/ftest".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
            [
                "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET, self.bucket_prefix),
                "gs://{}/{}/ftest/".format(TEST_GCS_BUCKET, self.bucket_prefix)
            ],
        ]

        gsutil_ls_set = set()
        gcs_ls_set = set()

        ls_path = "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix)

        copy_time_gsutil = 0
        copy_time_gcs = 0
        for i, test_case in enumerate(test_cases):
            print("{}/{} : Testcase: {}".format(i + 1, len(test_cases),
                                                test_case))
            if VERBOSE:
                print("gcs start...")
            start = time.time()
            call_gcs_client_cp(
                client=self.client,
                src=test_case[0],
                dst=test_case[1],
                recursive=False)
            end = time.time()
            copy_time_gcs += end - start
            gcs_paths = self.client.ls(gcs_url=ls_path, recursive=True)
            [gcs_ls_set.add(path) for path in gcs_paths]
            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

            if VERBOSE:
                print("gsutil start...")
            start = time.time()
            call_gsutil_cp(src=test_case[0], dst=test_case[1], recursive=False)
            end = time.time()
            copy_time_gsutil += end - start
            gsutil_paths = self.client.ls(gcs_url=ls_path, recursive=True)
            [gsutil_ls_set.add(path) for path in gsutil_paths]
            call_gsutil_rm(
                path="gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)
            gcs_test_setup(prefix=self.bucket_prefix)

            self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

        if VERBOSE:
            print("time gsutil: {}, gcs: {}".format(copy_time_gsutil,
                                                    copy_time_gcs))
        self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_cp_no_clobber(self):
        test_case = [
            "gs://{}/{}/d1/d11/f111".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
            "gs://{}/{}/play/d2/ff".format(TEST_GCS_BUCKET, self.bucket_prefix)
        ]

        path_f111 = gswrap._UniformPath(res_loc=test_case[0])
        path_ff = gswrap._UniformPath(res_loc=test_case[1])
        blob_f11 = self.client._bucket.get_blob(
            path_f111.prefix._convert_to_posix(remove_leading_backslash=True))
        blob_ff = self.client._bucket.get_blob(
            path_ff.prefix._convert_to_posix(remove_leading_backslash=True))

        timestamp_f11 = blob_f11.updated
        timestamp_ff = blob_ff.updated

        self.client.cp(src=test_case[0], dst=test_case[1], no_clobber=True)

        blob_f11_not_updated = self.client._bucket.get_blob(
            path_f111.prefix._convert_to_posix(remove_leading_backslash=True))
        blob_ff_not_updated = self.client._bucket.get_blob(
            path_ff.prefix._convert_to_posix(remove_leading_backslash=True))

        timestamp_f11_not_updated = blob_f11_not_updated.updated
        timestamp_ff_not_updated = blob_ff_not_updated.updated

        self.assertEqual(timestamp_f11, timestamp_f11_not_updated)
        self.assertEqual(timestamp_ff, timestamp_ff_not_updated)

    def test_cp_clobber(self):
        test_case = [
            "gs://{}/{}/d1/d11/f111".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
            "gs://{}/{}/play/d2/ff".format(TEST_GCS_BUCKET, self.bucket_prefix)
        ]

        path_f111 = gswrap._UniformPath(res_loc=test_case[0])
        path_ff = gswrap._UniformPath(res_loc=test_case[1])
        blob_f11 = self.client._bucket.get_blob(
            path_f111.prefix._convert_to_posix(remove_leading_backslash=True))
        blob_ff = self.client._bucket.get_blob(
            path_ff.prefix._convert_to_posix(remove_leading_backslash=True))

        timestamp_f11 = blob_f11.updated
        timestamp_ff = blob_ff.updated

        self.client.cp(src=test_case[0], dst=test_case[1], no_clobber=False)

        blob_f11_not_updated = self.client._bucket.get_blob(
            path_f111.prefix._convert_to_posix(remove_leading_backslash=True))
        blob_ff_updated = self.client._bucket.get_blob(
            path_ff.prefix._convert_to_posix(remove_leading_backslash=True))

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

            test_cases = [
                [
                    local_file.as_posix(), "gs://{}/{}/ftest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_file.as_posix(), "gs://{}/{}/ftest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path_posix, "gs://{}/{}/ftest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path_posix, "gs://{}/{}/local-file".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path_posix, "gs://{}/{}/ftest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path.as_posix(), "gs://{}/{}/dtest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path.as_posix(), "gs://{}/{}/dtest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path.as_posix() + '/', "gs://{}/{}/dtest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path.as_posix() + '/', "gs://{}/{}/dtest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
            ]

            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix)

            copy_time_gsutil = 0
            copy_time_gcs = 0
            for i, test_case in enumerate(test_cases):
                print("{}/{} : Testcase: {}".format(i + 1, len(test_cases),
                                                    test_case))

                if VERBOSE:
                    print("gcs start...")
                start = time.time()
                call_gcs_client_cp(
                    client=self.client,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=True)
                end = time.time()
                copy_time_gcs += end - start
                gcs_paths = self.client.ls(gcs_url=ls_path, recursive=True)
                [gcs_ls_set.add(path) for path in gcs_paths]
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)

                if VERBOSE:
                    print("gsutil start...")
                start = time.time()
                call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=True)
                end = time.time()
                copy_time_gsutil += end - start
                gsutil_paths = self.client.ls(gcs_url=ls_path, recursive=True)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)

                self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

            if VERBOSE:
                print("time gsutil: {}, gcs: {}".format(copy_time_gsutil,
                                                        copy_time_gcs))

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

            test_cases = [
                [
                    local_file.as_posix(), "gs://{}/{}/ftest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_file.as_posix(), "gs://{}/{}/ftest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path_posix, "gs://{}/{}/ftest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path_posix, "gs://{}/{}/local-file".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path_posix, "gs://{}/{}/ftest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path.as_posix(), "gs://{}/{}/dtest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path.as_posix(), "gs://{}/{}/dtest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path.as_posix() + '/', "gs://{}/{}/dtest".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
                [
                    local_path.as_posix() + '/', "gs://{}/{}/dtest/".format(
                        TEST_GCS_BUCKET, self.bucket_prefix)
                ],
            ]

            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = "gs://{}/{}/".format(TEST_GCS_BUCKET, self.bucket_prefix)

            copy_time_gsutil = 0
            copy_time_gcs = 0
            for i, test_case in enumerate(test_cases):
                print("{}/{} : Testcase: {}".format(i + 1, len(test_cases),
                                                    test_case))

                if VERBOSE:
                    print("gcs start...")
                start = time.time()
                call_gcs_client_cp(
                    client=self.client,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=False)
                end = time.time()
                copy_time_gcs += end - start
                gcs_paths = self.client.ls(gcs_url=ls_path, recursive=True)
                [gcs_ls_set.add(path) for path in gcs_paths]
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)

                if VERBOSE:
                    print("gsutil start...")
                start = time.time()
                call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=False)
                end = time.time()
                copy_time_gsutil += end - start
                gsutil_paths = self.client.ls(gcs_url=ls_path, recursive=True)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                call_gsutil_rm(
                    path="gs://{}/{}/".format(TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                gcs_test_setup(prefix=self.bucket_prefix)

                self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

            if VERBOSE:
                print("time gsutil: {}, gcs: {}".format(copy_time_gsutil,
                                                        copy_time_gcs))

            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

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
        if files:
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

            test_cases = [
                [
                    "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                    (local_path / 'uninitialized-file').as_posix()
                ],
                [
                    "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                    local_file.as_posix()
                ],
                [
                    "gs://{}/{}/d3/d31/d311".format(TEST_GCS_BUCKET,
                                                    self.bucket_prefix),
                    local_file.as_posix()
                ],  # NotADirectoryError
                [
                    "gs://{}/{}/d3/d31/d311/".format(TEST_GCS_BUCKET,
                                                     self.bucket_prefix),
                    local_file.as_posix()
                ],  # NotADirectoryError
                [
                    "gs://{}/{}/d1/".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
                    local_dir.as_posix()
                ],
                [
                    "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
                    local_dir.as_posix()
                ],
                [
                    "gs://{}/{}/d1/".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
                    local_dir.as_posix() + '/'
                ],
                [
                    "gs://{}/{}/d1/".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
                    local_dir.as_posix() + '/'
                ],
            ]

            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = tmp_dir.path.as_posix()

            copy_time_gsutil = 0
            copy_time_gcs = 0
            for i, test_case in enumerate(test_cases):
                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                print("{}/{} : Testcase: {}".format(i + 1, len(test_cases),
                                                    test_case))
                if VERBOSE:
                    print("gcs start...")
                start = time.time()
                try:
                    call_gcs_client_cp(
                        client=self.client,
                        src=test_case[0],
                        dst=test_case[1],
                        recursive=True)
                except NotADirectoryError:
                    pass
                end = time.time()
                copy_time_gcs += end - start
                gcs_paths = ls_local(path=ls_path)
                [gcs_ls_set.add(path) for path in gcs_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                if VERBOSE:
                    print("gsutil start...")
                start = time.time()
                call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=True)
                end = time.time()
                copy_time_gsutil += end - start
                gsutil_paths = ls_local(path=ls_path)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                self.assertListEqual(gsutil_paths, gcs_paths)

            if VERBOSE:
                print("time gsutil: {}, gcs: {}".format(copy_time_gsutil,
                                                        copy_time_gcs))
            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

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

            test_cases = [
                [
                    "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                    (local_path / 'uninitialized-file').as_posix()
                ],
                [
                    "gs://{}/{}/d1/f11".format(TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                    local_file.as_posix()
                ],
                [
                    "gs://{}/{}/d3/d31/d311".format(TEST_GCS_BUCKET,
                                                    self.bucket_prefix),
                    local_file.as_posix()
                ],  # NotADirectoryError
                [
                    "gs://{}/{}/d3/d31/d311/".format(TEST_GCS_BUCKET,
                                                     self.bucket_prefix),
                    local_file.as_posix()
                ],  # NotADirectoryError
                [
                    "gs://{}/{}/d1/".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
                    local_dir.as_posix()
                ],
                [
                    "gs://{}/{}/d1".format(TEST_GCS_BUCKET, self.bucket_prefix),
                    local_dir.as_posix()
                ],
                [
                    "gs://{}/{}/d1/".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
                    local_dir.as_posix() + '/'
                ],
                [
                    "gs://{}/{}/d1/".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
                    local_dir.as_posix() + '/'
                ],
            ]

            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = tmp_dir.path.as_posix()

            copy_time_gsutil = 0
            copy_time_gcs = 0
            for i, test_case in enumerate(test_cases):
                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                print("{}/{} : Testcase: {}".format(i + 1, len(test_cases),
                                                    test_case))
                if VERBOSE:
                    print("gcs start...")
                start = time.time()
                try:
                    call_gcs_client_cp(
                        client=self.client,
                        src=test_case[0],
                        dst=test_case[1],
                        recursive=False)
                except NotADirectoryError:
                    pass
                end = time.time()
                copy_time_gcs += end - start
                gcs_paths = ls_local(path=ls_path)
                [gcs_ls_set.add(path) for path in gcs_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                if VERBOSE:
                    print("gsutil start...")
                start = time.time()
                call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=False)
                end = time.time()
                copy_time_gsutil += end - start
                gsutil_paths = ls_local(path=ls_path)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                self.assertListEqual(gsutil_paths, gcs_paths)

            if VERBOSE:
                print("time gsutil: {}, gcs: {}".format(copy_time_gsutil,
                                                        copy_time_gcs))
            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

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
            test_cases = [[
                local_file.as_posix(),
                (local_path / 'another-local-file').as_posix()
            ], [local_dir.as_posix(), dst_dir.as_posix()]]

            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = tmp_dir.path.as_posix()

            copy_time_gsutil = 0
            copy_time_gcs = 0
            for recursive in [True, False]:
                for i, test_case in enumerate(test_cases):
                    print("{}/{} : Testcase: {}, recursive: {}".format(
                        i + 1, len(test_cases), test_case, recursive))

                    if VERBOSE:
                        print("gsutil start...")
                    start = time.time()
                    call_gsutil_cp(
                        src=test_case[0], dst=test_case[1], recursive=recursive)
                    end = time.time()
                    copy_time_gsutil += end - start
                    gsutil_paths = ls_local(path=ls_path)
                    [gsutil_ls_set.add(path) for path in gsutil_paths]
                    if pathlib.Path(test_case[1]).is_dir():
                        shutil.rmtree(test_case[1], True)
                    elif pathlib.Path(test_case[1]).exists():
                        os.remove(test_case[1])

                    if VERBOSE:
                        print("gcs start...")
                    start = time.time()
                    try:
                        if pathlib.Path(test_case[0]).is_dir() and pathlib.Path(
                                test_case[1]).is_dir():
                            shutil.rmtree(test_case[1])
                        call_gcs_client_cp(
                            client=self.client,
                            src=test_case[0],
                            dst=test_case[1],
                            recursive=recursive)
                    except NotADirectoryError:
                        pass
                    end = time.time()
                    copy_time_gcs += end - start
                    gcs_paths = ls_local(path=ls_path)
                    [gcs_ls_set.add(path) for path in gcs_paths]
                    if pathlib.Path(test_case[1]).is_dir():
                        shutil.rmtree(test_case[1], True)
                    elif pathlib.Path(test_case[1]).exists():
                        os.remove(test_case[1])

                    self.assertListEqual(gsutil_paths, gcs_paths)

                if VERBOSE:
                    print("time gsutil: {}, gcs: {}".format(
                        copy_time_gsutil, copy_time_gcs))
                self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

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


def call_gcs_client_rm(client: gswrap.Client,
                       gcs_url: str,
                       recursive: bool = False):
    try:
        client.rm(gcs_url=gcs_url, recursive=recursive)
    except google.api_core.exceptions.GoogleAPIError as err:
        print(err)
    except ValueError as valerr:
        print(valerr)


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

            parent = gswrap._GCSPathlib(path=local_tmpdir.path.as_posix(
            ))._name_of_parent_dir()._convert_to_posix(
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

            blobs_erased = self.client.ls(
                gcs_url='gs://{}/{}/{}'.format(TEST_GCS_BUCKET,
                                               self.bucket_prefix, parent),
                recursive=True)
            self.assertEqual(0, len(blobs_erased), "Blobs were not erased.")

    def test_gsutil_vs_gswrap_remove_recursive(self):  # pylint: disable=invalid-name
        test_cases = [
            "gs://{}/{}/d1/d11/f111".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
            "gs://{}/{}/d1/d11".format(
                TEST_GCS_BUCKET, self.bucket_prefix), "gs://{}/{}/d1/".format(
                    TEST_GCS_BUCKET, self.bucket_prefix), "gs://{}/{}/d".format(
                        TEST_GCS_BUCKET,
                        self.bucket_prefix), "gs://{}/{}/d/".format(
                            TEST_GCS_BUCKET,
                            self.bucket_prefix), "gs://{}/{}".format(
                                TEST_GCS_BUCKET, self.bucket_prefix)
        ]

        for i, test_case in enumerate(test_cases):
            print("{}/{} : Testcase: {}".format(i + 1, len(test_cases),
                                                test_case))
            gcs_test_setup(prefix=self.bucket_prefix)
            call_gcs_client_rm(
                client=self.client, gcs_url=test_case, recursive=True)
            list_gcs = call_gsutil_ls(
                path="gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            gcs_test_setup(prefix=self.bucket_prefix)
            call_gsutil_rm(path=test_case, recursive=True)
            list_gsutil = call_gsutil_ls(
                path="gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            print(sorted(list_gsutil))
            print(sorted(list_gcs))
            self.assertListEqual(sorted(list_gsutil), sorted(list_gcs))

    def test_gsutil_vs_gswrap_remove_non_recursive(self):  # pylint: disable=invalid-name
        test_cases = [
            "gs://{}/{}/d1/d11/f111".format(TEST_GCS_BUCKET,
                                            self.bucket_prefix),
            "gs://{}/{}/d1/d11".format(
                TEST_GCS_BUCKET, self.bucket_prefix), "gs://{}/{}/d1/".format(
                    TEST_GCS_BUCKET, self.bucket_prefix), "gs://{}/{}/d".format(
                        TEST_GCS_BUCKET,
                        self.bucket_prefix), "gs://{}/{}/d/".format(
                            TEST_GCS_BUCKET,
                            self.bucket_prefix), "gs://{}/{}".format(
                                TEST_GCS_BUCKET, self.bucket_prefix)
        ]

        for i, test_case in enumerate(test_cases):
            print("{}/{} : Testcase: {}".format(i + 1, len(test_cases),
                                                test_case))
            gcs_test_setup(prefix=self.bucket_prefix)
            call_gcs_client_rm(
                client=self.client, gcs_url=test_case, recursive=False)
            list_gcs = call_gsutil_ls(
                path="gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            gcs_test_setup(prefix=self.bucket_prefix)
            call_gsutil_rm(path=test_case, recursive=False)
            list_gsutil = call_gsutil_ls(
                path="gs://{}/{}".format(TEST_GCS_BUCKET, self.bucket_prefix),
                recursive=True)

            print(sorted(list_gsutil))
            print(sorted(list_gcs))
            self.assertListEqual(sorted(list_gsutil), sorted(list_gcs))


if __name__ == '__main__':
    unittest.main()
