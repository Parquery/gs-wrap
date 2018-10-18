#!/usr/bin/env python
"""Test gs-wrap."""

# pylint: disable=missing-docstring

import google.api_core.exceptions
import os
import pathlib
import time
from typing import List
import shutil
import subprocess
import unittest
import warnings

import temppathlib
import uuid

import gs_wrap


class TestURL(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings(
            "ignore",
            category=UserWarning,
            message="Your application has authenticated using end user credentials from Google Cloud SDK.*")
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
        self.client = gs_wrap.GoogleCloudStorageClient(bucket_name='parquery-sandbox')

    def test_contains_wildcard(self):
        no_wildcard = 'no wildcard here'
        asterisk = '*/somedir'
        questionmark = 'f?lder'
        self.assertFalse(self.client._contains_wildcard(prefix=no_wildcard))
        self.assertTrue(self.client._contains_wildcard(prefix=asterisk))
        self.assertTrue(self.client._contains_wildcard(prefix=questionmark))
        double_asterisk = 'folder/**/another-folder'
        self.assertTrue(self.client._contains_wildcard(prefix=double_asterisk))

    def test_gcs_url_decomposition(self):
        bucket = 'parquery-sandbox'
        prefix = 'gsutil_playground/d1'
        link = 'gs://' + bucket + '/' + prefix
        url = gs_wrap.UniformPath(link=link)

        self.assertEqual(bucket, url.bucket)
        self.assertEqual(prefix, url.prefix.convert_to_posix(add_trailing_backslash=url.prefix.had_trailing_backslash, remove_leading_backslash=True, is_cloud_URL=True))
        self.assertTrue(url.is_cloud_URL)

    def test_local_url_decomposition(self):
        bucket = None
        prefix = '/user/home/'
        link = prefix
        url = gs_wrap.UniformPath(link=link)

        self.assertEqual(bucket, url.bucket)
        self.assertEqual(prefix, url.prefix.convert_to_posix(add_trailing_backslash=url.prefix.had_trailing_backslash, is_cloud_URL=False))
        self.assertFalse(url.is_cloud_URL)


class TestCopy(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings(
            "ignore",
            category=UserWarning,
            message="Your application has authenticated using end user credentials from Google Cloud SDK.*")
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

        self.client = gs_wrap.GoogleCloudStorageClient(bucket_name='parquery-sandbox')

    def test_upload(self):

        with temppathlib.TemporaryDirectory() as local_tmpdir:
            tmp_folder = local_tmpdir.path / 'some-folder'
            tmp_folder.mkdir()
            tmp_path = tmp_folder / str(uuid.uuid4())
            tmp_path.write_text('hello')
            other_folder = local_tmpdir.path / 'another-folder'
            other_folder.mkdir()
            other_file = other_folder / str(uuid.uuid4())
            other_file.write_text('hello')

            print("copy posix:", local_tmpdir.path.as_posix())
            self.client.cp(src=local_tmpdir.path.as_posix(), dest='gs://parquery-sandbox/gsutil_playground/')

            print("expected path: {}/{}".format("gsutil_playground", tmp_path.relative_to(local_tmpdir.path.parent)))
            content = self.client.get_bucket().get_blob(
                blob_name="{}/{}".format("gsutil_playground", tmp_path.relative_to(local_tmpdir.path.parent)))
            content_other_file = self.client.get_bucket().get_blob(
                blob_name="{}/{}".format("gsutil_playground", other_file.relative_to(local_tmpdir.path.parent)))
            text = content.download_as_string()
            text_other_file = content_other_file.download_as_string()
            self.assertEqual(b'hello', text)
            self.assertEqual(b'hello', text_other_file)

            content.delete()
            content_other_file.delete()

    def test_copy_folder_in_same_bucket(self):
        src = 'gs://parquery-sandbox/gsutil_playground/d1/'
        dest = 'gs://parquery-sandbox/gsutil_playground/dtest1/'

        self.client.cp(src=src, dest=dest)

        src_list = self.client.ls(link=src, dont_recurse=False)
        dest_list = self.client.ls(link=dest, dont_recurse=False)

        for src_file, dest_file in zip(src_list, dest_list):
            src_file = src_file.replace(src, dest + 'd1/')
            self.assertEqual(src_file, dest_file)

        self.client.rm(link=dest)

        src2 = 'gs://parquery-sandbox/gsutil_playground/d2'
        dest2 = 'gs://parquery-sandbox/gsutil_playground/dtest2/'

        self.client.cp(src=src2, dest=dest2)

        src_list2 = self.client.ls(link=src2, dont_recurse=False)
        dest_list2 = self.client.ls(link=dest2, dont_recurse=False)

        for src_file, dest_file in zip(src_list2, dest_list2):
            src_file = src_file.replace(src2, dest2 + 'd2')
            self.assertEqual(src_file, dest_file)

        self.client.rm(link=dest2)

    def test_copy_subfolders_and_files_in_same_bucket(self):
        src = 'gs://parquery-sandbox/gsutil_playground/d1/'
        dest = 'gs://parquery-sandbox/gsutil_playground/dtest1'

        self.client.cp(src=src, dest=dest)

        src_list = self.client.ls(link=src, dont_recurse=False)
        dest_list = self.client.ls(link=dest, dont_recurse=False)

        for src_file, dest_file in zip(src_list, dest_list):
            src_file = src_file.replace(src, dest + '/')
            self.assertEqual(src_file, dest_file)

        self.client.rm(link=dest)

        src2 = 'gs://parquery-sandbox/gsutil_playground/d2'
        dest2 = 'gs://parquery-sandbox/gsutil_playground/dtest2'

        self.client.cp(src=src2, dest=dest2)

        src_list2 = self.client.ls(link=src2, dont_recurse=False)
        dest_list2 = self.client.ls(link=dest2, dont_recurse=False)

        for src_file, dest_file in zip(src_list2, dest_list2):
            src_file = src_file.replace(src2, dest2)
            self.assertEqual(src_file, dest_file)

        self.client.rm(link=dest2)

    def test_download(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            local_path = local_tmpdir.path / 'folder'
            local_path.mkdir()
            self.client.cp(src='gs://parquery-sandbox/gsutil_playground/d1/', dest=local_path.as_posix())

            file_pth = local_path / 'd1' / 'f11'
            downloaded_text = file_pth.read_bytes()
            content = self.client.get_bucket().get_blob(blob_name="gsutil_playground/d1/f11")
            text = content.download_as_string()

            self.assertEqual(text, downloaded_text)


class TestCreateRemove(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings(
            "ignore",
            category=UserWarning,
            message="Your application has authenticated using end user credentials from Google Cloud SDK.*")
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

        self.client = gs_wrap.GoogleCloudStorageClient(bucket_name='parquery-sandbox')

    def test_remove_blob(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            tmp_folder = local_tmpdir.path / 'some-folder'
            tmp_folder.mkdir()
            tmp_path = tmp_folder / str(uuid.uuid4())
            tmp_path.write_text('hello')

            self.client.cp(src=local_tmpdir.path.as_posix(), dest='gs://parquery-sandbox/gsutil_playground/')

            blobs = self.client.ls(link='gs://parquery-sandbox/gsutil_playground/some-folder')
            self.assertEqual(1, len(blobs), "More or less blobs found.")

            self.client.rm(link='gs://parquery-sandbox/gsutil_playground/some-folder')

            blobs_erased = self.client.ls(link='gs://parquery-sandbox/gsutil_playground/some-folder')
            self.assertEqual(0, len(blobs_erased), "Blobs were not erased.")


class TestGsutilvsGCSClient(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings(
            "ignore",
            category=UserWarning,
            message="Your application has authenticated using end user credentials from Google Cloud SDK.*")
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
        self.client = gs_wrap.GoogleCloudStorageClient(bucket_name='parquery-sandbox')

    @staticmethod
    def call_gsutil_ls(path: str) -> List[str]:
        cmd = ["gsutil", "ls", path]

        proc = subprocess.Popen(cmd, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        lines = [line.strip() for line in str(stdout).splitlines() if line.strip()]
        return lines

    @staticmethod
    def call_gcs_client_ls(client: gs_wrap.GoogleCloudStorageClient, path: str) -> List[str]:

        try:
            list_ls = client.ls(link=path)
        except google.api_core.exceptions.GoogleAPIError as err:
            print(err)
            return []

        return list_ls

    def test_gsutil_ls(self):
        test_cases = [
            # no wildcards
            "gs://parquery-inexistent",
            "gs://parquery-data",
            "gs://parquery-sandbo",
            "gs://parquery-sandbox",
            "gs://parquery-sandbox/",
            "gs://parquery-sandbox/gsutil_playgro",
            "gs://parquery-sandbox/gsutil_playground",
            "gs://parquery-sandbox/gsutil_playground/",
            "gs://parquery-sandbox/gsutil_playground/d",
            "gs://parquery-sandbox/gsutil_playground/d1",
            "gs://parquery-sandbox/gsutil_playground/d1/"
        ]

        for i, test_case in enumerate(test_cases):
            print("{}/{}".format(i + 1, len(test_cases)))
            start_gsutil = time.time()
            list_gsutil = self.call_gsutil_ls(path=test_case)
            end_gsutil = time.time()
            start_gcs = time.time()
            list_gcs = self.call_gcs_client_ls(client=self.client, path=test_case)
            end_gcs = time.time()
            print('Testcase: {}, Time: gsutil: {}, gcs: {}'.format(test_case, end_gsutil - start_gsutil,
                                                                   end_gcs - start_gcs))
            print('Gsutil: ', list_gsutil)
            print('GCS:    ', list_gcs)
            print('#########################')

            self.assertListEqual(list_gsutil, list_gcs)

    @staticmethod
    def call_gsutil_cp(src: str, dest: str):
        cmd = ["gsutil", "-m", "cp", "-r", src, dest]

        proc = subprocess.Popen(cmd, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc.communicate()

    @staticmethod
    def call_gsutil_rm(path: str):
        cmd = ["gsutil", "-m", "rm", "-r", path]

        proc = subprocess.Popen(cmd, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc.communicate()

    @staticmethod
    def call_gcs_client_cp(client: gs_wrap.GoogleCloudStorageClient, src: str, dest: str):

        try:
            client.cp(src=src, dest=dest)
        except google.api_core.exceptions.GoogleAPIError as err:
            print(err)

    def test_copy(self):
        test_cases = [
            ["gs://parquery-sandbox/gsutil_playground/d1", "gs://parquery-sandbox/gsutil_playground/dtest/"],
            ["gs://parquery-sandbox/gsutil_playground/d1/", "gs://parquery-sandbox/gsutil_playground/dtest/"],
            ["gs://parquery-sandbox/gsutil_playground/d1", "gs://parquery-sandbox/gsutil_playground/dtest"],
            ["gs://parquery-sandbox/gsutil_playground/d1/", "gs://parquery-sandbox/gsutil_playground/dtest"],
            [
                "gs://parquery-sandbox/gsutil_playground/d3/d31/d311/f3111",
                "gs://parquery-sandbox/gsutil_playground/ftest"
            ],
            [
                "gs://parquery-sandbox/gsutil_playground/d3/d31/d311/f3111",
                "gs://parquery-sandbox/gsutil_playground/ftest/"
            ],
            ["gs://parquery-sandbox/gsutil_playground/d1/f11", "gs://parquery-sandbox/gsutil_playground/ftest"],
            ["gs://parquery-sandbox/gsutil_playground/d1/f11", "gs://parquery-sandbox/gsutil_playground/ftest/"],
        ]

        gsutil_ls_set = set()
        gcs_ls_set = set()

        ls_path = "gs://parquery-sandbox/gsutil_playground/"

        copy_time_gsutil = 0
        copy_time_gcs = 0
        for i, test_case in enumerate(test_cases):
            print("{}/{} : Testcase: {}".format(i + 1, len(test_cases), test_case))

            print("gcs start...")
            start = time.time()
            self.call_gcs_client_cp(client=self.client, src=test_case[0], dest=test_case[1])
            end = time.time()
            copy_time_gcs += end - start
            gcs_paths = self.client.ls(link=ls_path, dont_recurse=False)
            [gcs_ls_set.add(path) for path in gcs_paths]
            self.call_gsutil_rm(path=test_case[1])

            print("gsutil start...")
            start = time.time()
            self.call_gsutil_cp(src=test_case[0], dest=test_case[1])
            end = time.time()
            copy_time_gsutil += end - start
            gsutil_paths = self.client.ls(link=ls_path, dont_recurse=False)
            [gsutil_ls_set.add(path) for path in gsutil_paths]
            self.call_gsutil_rm(path=test_case[1])

            self.assertListEqual(gsutil_paths, gcs_paths)

        print("time gsutil: {}, gcs: {}".format(copy_time_gsutil, copy_time_gcs))
        self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_upload(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            local_path = tmp_dir.path / 'tmp'
            local_path.mkdir()
            local_file = local_path / 'local-file'
            local_file.write_text('hello')
            local_path_posix = local_file.as_posix()
            another_local_file = local_path / 'another-local-file'
            another_local_file.write_text('hello again')

            test_cases = [
                [local_file.as_posix(), "gs://parquery-sandbox/gsutil_playground/ftest/"],
                [local_file.as_posix(), "gs://parquery-sandbox/gsutil_playground/ftest"],
                [local_path_posix, "gs://parquery-sandbox/gsutil_playground/ftest/"],
                [local_path_posix, "gs://parquery-sandbox/gsutil_playground/local-file"],
                [local_path_posix, "gs://parquery-sandbox/gsutil_playground/ftest"],
                [local_path.as_posix(), "gs://parquery-sandbox/gsutil_playground/dtest"],
                [local_path.as_posix(), "gs://parquery-sandbox/gsutil_playground/dtest/"],
                [local_path.as_posix() + '/', "gs://parquery-sandbox/gsutil_playground/dtest"],
                [local_path.as_posix() + '/', "gs://parquery-sandbox/gsutil_playground/dtest/"],
            ]

            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = "gs://parquery-sandbox/gsutil_playground/"

            copy_time_gsutil = 0
            copy_time_gcs = 0
            for i, test_case in enumerate(test_cases):
                print("{}/{} : Testcase: {}".format(i + 1, len(test_cases), test_case))

                print("gcs start...")
                start = time.time()
                self.call_gcs_client_cp(client=self.client, src=test_case[0], dest=test_case[1])
                end = time.time()
                copy_time_gcs += end - start
                gcs_paths = self.client.ls(link=ls_path, dont_recurse=False)
                [gcs_ls_set.add(path) for path in gcs_paths]
                self.call_gsutil_rm(path=test_case[1])

                print("gsutil start...")
                start = time.time()
                self.call_gsutil_cp(src=test_case[0], dest=test_case[1])
                end = time.time()
                copy_time_gsutil += end - start
                gsutil_paths = self.client.ls(link=ls_path, dont_recurse=False)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                self.call_gsutil_rm(path=test_case[1])

                self.assertListEqual(gsutil_paths, gcs_paths)

            print("time gsutil: {}, gcs: {}".format(copy_time_gsutil, copy_time_gcs))
            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    @staticmethod
    def ls_local(path: str) -> List[str]:
        paths = []  # type: List[str]
        for root, dirs, files in os.walk(path):
            if files:
                for file in files:
                    paths.append(os.path.join(root, file))

        return paths

    def test_download(self):
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
                # TODO: ["gs://parquery-sandbox/gsutil_playground/d1/f11", (local_path / 'uninitialized-file').as_posix()],
                ["gs://parquery-sandbox/gsutil_playground/d1/f11",
                 local_file.as_posix()],
                ["gs://parquery-sandbox/gsutil_playground/d3/d31/d311",
                 local_file.as_posix()],  # NotADirectoryError
                ["gs://parquery-sandbox/gsutil_playground/d3/d31/d311/",
                 local_file.as_posix()],  # NotADirectoryError
                ["gs://parquery-sandbox/gsutil_playground/d1/",
                 local_dir.as_posix()],
                ["gs://parquery-sandbox/gsutil_playground/d1",
                 local_dir.as_posix()],
                ["gs://parquery-sandbox/gsutil_playground/d1/",
                 local_dir.as_posix() + '/'],
                ["gs://parquery-sandbox/gsutil_playground/d1/",
                 local_dir.as_posix() + '/'],
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

                print("{}/{} : Testcase: {}".format(i + 1, len(test_cases), test_case))

                print("gcs start...")
                start = time.time()
                try:
                    self.call_gcs_client_cp(client=self.client, src=test_case[0], dest=test_case[1])
                except NotADirectoryError:
                    pass
                end = time.time()
                copy_time_gcs += end - start
                gcs_paths = self.ls_local(path=ls_path)
                [gcs_ls_set.add(path) for path in gcs_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                print("gsutil start...")
                start = time.time()
                self.call_gsutil_cp(src=test_case[0], dest=test_case[1])
                end = time.time()
                copy_time_gsutil += end - start
                gsutil_paths = self.ls_local(path=ls_path)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                print(gsutil_paths)
                print(gcs_paths)
                self.assertListEqual(gsutil_paths, gcs_paths)

            print("time gsutil: {}, gcs: {}".format(copy_time_gsutil, copy_time_gcs))
            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_local_cp(self):
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
            dest_dir = local_path / 'dest-dir'
            dest_dir.mkdir()
            test_cases = [[local_file.as_posix(), (local_path / 'another-local-file').as_posix()],
                          [local_dir.as_posix(), dest_dir.as_posix()]]

            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = tmp_dir.path.as_posix()

            copy_time_gsutil = 0
            copy_time_gcs = 0
            for i, test_case in enumerate(test_cases):
                print("{}/{} : Testcase: {}".format(i + 1, len(test_cases), test_case))

                print("gsutil start...")
                start = time.time()
                self.call_gsutil_cp(src=test_case[0], dest=test_case[1])
                end = time.time()
                copy_time_gsutil += end - start
                gsutil_paths = self.ls_local(path=ls_path)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)
                else:
                    os.remove(test_case[1])

                print("gcs start...")
                start = time.time()
                try:
                    if pathlib.Path(test_case[0]).is_dir() and pathlib.Path(test_case[1]).is_dir():
                        shutil.rmtree(test_case[1])
                    self.call_gcs_client_cp(client=self.client, src=test_case[0], dest=test_case[1])
                except NotADirectoryError:
                    pass
                end = time.time()
                copy_time_gcs += end - start
                gcs_paths = self.ls_local(path=ls_path)
                [gcs_ls_set.add(path) for path in gcs_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)
                else:
                    os.remove(test_case[1])

                self.assertListEqual(gsutil_paths, gcs_paths)

            print("time gsutil: {}, gcs: {}".format(copy_time_gsutil, copy_time_gcs))
            self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))


if __name__ == '__main__':
    unittest.main()
