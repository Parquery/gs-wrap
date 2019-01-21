#!/usr/bin/env python
"""Test gs-wrap cp download live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

import datetime
import pathlib
import shutil
import subprocess
import tempfile
import unittest
import uuid

import google.api_core.exceptions
import temppathlib

import gswrap
import tests.common


class TestCPDownload(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client()
        self.client._change_bucket(tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        self.tmp_dir = tempfile.TemporaryDirectory()
        tests.common.gcs_test_setup(
            tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

    def tearDown(self):
        tests.common.gcs_test_teardown(prefix=self.bucket_prefix)
        self.tmp_dir.cleanup()

    def test_download_one_dir(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            local_path = local_tmpdir.path / 'folder'
            local_path.mkdir()
            self.client.cp(
                src='gs://{}/{}/d1/'.format(tests.common.TEST_GCS_BUCKET,
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
                ["gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix)
                    , (local_path / 'uninitialized-file').as_posix()],
                ["gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix)
                    , local_file.as_posix()],
                ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
                 local_dir.as_posix()],
                ["gs://{}/{}/d1".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix),
                    local_dir.as_posix()],
                ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
                 local_dir.as_posix() + '/'],
                ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
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

                gcs_paths = tests.common.ls_local(path=ls_path)
                [gcs_ls_set.add(path) for path in gcs_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                tests.common.call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=True)
                gsutil_paths = tests.common.ls_local(path=ls_path)
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
                ["gs://{}/{}/d3/d31/d311".format(tests.common.TEST_GCS_BUCKET,
                                                 self.bucket_prefix),
                 local_file.as_posix()],
                ["gs://{}/{}/d3/d31/d311/".format(tests.common.TEST_GCS_BUCKET,
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
                    tests.common.call_gsutil_cp,
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
                ["gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix)
                    , (local_path / 'uninitialized-file').as_posix()],
                ["gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix)
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

                gcs_paths = tests.common.ls_local(path=ls_path)
                [gcs_ls_set.add(path) for path in gcs_paths]
                if pathlib.Path(test_case[1]).is_dir():
                    shutil.rmtree(test_case[1], True)

                if not local_dir.exists():
                    local_dir = local_path / 'local-dir'
                    local_dir.mkdir()

                tests.common.call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=False)
                gsutil_paths = tests.common.ls_local(path=ls_path)
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
                ["gs://{}/{}/d3/d31/d311".format(tests.common.TEST_GCS_BUCKET,
                                                 self.bucket_prefix),
                 local_file.as_posix()],  # NotADirectoryError
                ["gs://{}/{}/d3/d31/d311/".format(tests.common.TEST_GCS_BUCKET,
                                                  self.bucket_prefix),
                 local_file.as_posix()],  # NotADirectoryError
                ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
                 local_dir.as_posix()],
                ["gs://{}/{}/d1".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix),
                 local_dir.as_posix()],
                ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
                 local_dir.as_posix() + '/'],
                ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
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
                    tests.common.call_gsutil_cp,
                    src=test_case[0],
                    dst=test_case[1],
                    recursive=False)

    def test_download_no_clobber(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            local_path = local_tmpdir.path / 'file'
            local_path.write_text("don't overwrite")

            self.client.cp(
                src='gs://{}/{}/d1/f11'.format(tests.common.TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                dst=local_path.as_posix(),
                no_clobber=True)

            self.assertEqual("don't overwrite", local_path.read_text())

    def test_download_clobber(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            local_path = local_tmpdir.path / 'file'
            local_path.write_text("overwrite file")

            self.client.cp(
                src='gs://{}/{}/d1/f11'.format(tests.common.TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                dst=local_path.as_posix(),
                no_clobber=False)

            self.assertEqual(tests.common.GCS_FILE_CONTENT,
                             local_path.read_text())


class TestCPDownloadNoCommonSetup(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client()
        self.client._change_bucket(tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())

    def tearDown(self):
        pass

    def test_download_preserved_posix(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            setup_file = tmp_dir.path / 'file-to-download'
            setup_file.write_text(tests.common.GCS_FILE_CONTENT)
            url = 'gs://{}/{}/d1/f11'.format(tests.common.TEST_GCS_BUCKET,
                                             self.bucket_prefix)

            subprocess.check_call(
                ["gsutil", "cp", "-P",
                 setup_file.as_posix(), url])

            file = tmp_dir.path / 'file'

            self.client.cp(
                src=url,
                dst=file.as_posix(),
                recursive=True,
                preserve_posix=True)

            try:
                gcs_stat = self.client.stat(url=url)
                self.assertIsNotNone(gcs_stat)

                file_stat = file.stat()
                self.assertIsNotNone(file_stat)

                self.assertEqual(file_stat.st_size, gcs_stat.content_length)

                self.assertEqual(
                    datetime.datetime.utcfromtimestamp(
                        file_stat.st_mtime).replace(microsecond=0).timestamp(),
                    gcs_stat.file_mtime.timestamp())

                self.assertEqual(file_stat.st_uid, int(gcs_stat.posix_uid))
                self.assertEqual(file_stat.st_gid, int(gcs_stat.posix_gid))
                self.assertEqual(gcs_stat.posix_mode,
                                 oct(file_stat.st_mode)[-3:])
            finally:
                tests.common.call_gsutil_rm(path=url, recursive=False)


if __name__ == '__main__':
    unittest.main()
