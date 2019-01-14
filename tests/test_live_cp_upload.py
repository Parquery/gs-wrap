#!/usr/bin/env python
"""Test gs-wrap cp upload live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

import datetime
import subprocess
import tempfile
import unittest
import uuid

import temppathlib

import gswrap
import tests.common


class TestCPUpload(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        self.tmp_dir = tempfile.TemporaryDirectory()
        tests.common.gcs_test_setup(
            tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

    def tearDown(self):
        tests.common.gcs_test_teardown(prefix=self.bucket_prefix)
        self.tmp_dir.cleanup()

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
                    tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_file.as_posix(), "gs://{}/{}/ftest".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/ftest/".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/local-file".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/ftest".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix(), "gs://{}/{}/dtest".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix(), "gs://{}/{}/dtest/".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix() + '/', "gs://{}/{}/dtest".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix() + '/', "gs://{}/{}/dtest/".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
            ]
            # yapf: enable
            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = "gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)

            for test_case in test_cases:
                self.client.cp(
                    src=test_case[0], dst=test_case[1], recursive=True)
                gcs_paths = self.client.ls(url=ls_path, recursive=True)
                [gcs_ls_set.add(path) for path in gcs_paths]
                tests.common.call_gsutil_rm(
                    path="gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                tests.common.gcs_test_setup(
                    tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

                tests.common.call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=True)
                gsutil_paths = self.client.ls(url=ls_path, recursive=True)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                tests.common.call_gsutil_rm(
                    path="gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                tests.common.gcs_test_setup(
                    tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

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
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_file.as_posix(), "gs://{}/{}/ftest".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/ftest/".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/local-file".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path_posix, "gs://{}/{}/ftest".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
            ]
            # yapf: enable
            gsutil_ls_set = set()
            gcs_ls_set = set()

            ls_path = "gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)

            for test_case in test_cases:
                self.client.cp(
                    src=test_case[0], dst=test_case[1], recursive=False)
                gcs_paths = self.client.ls(url=ls_path, recursive=True)
                [gcs_ls_set.add(path) for path in gcs_paths]
                tests.common.call_gsutil_rm(
                    path="gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                tests.common.gcs_test_setup(
                    tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)
                tests.common.call_gsutil_cp(
                    src=test_case[0], dst=test_case[1], recursive=False)
                gsutil_paths = self.client.ls(url=ls_path, recursive=True)
                [gsutil_ls_set.add(path) for path in gsutil_paths]
                tests.common.call_gsutil_rm(
                    path="gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                              self.bucket_prefix),
                    recursive=True)
                tests.common.gcs_test_setup(
                    tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

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
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix(), "gs://{}/{}/dtest/".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix() + '/', "gs://{}/{}/dtest".format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
                [local_path.as_posix() + '/', "gs://{}/{}/dtest/".format(
                       tests.common.TEST_GCS_BUCKET, self.bucket_prefix)],
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
                    tests.common.call_gsutil_cp,
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
                dst='gs://{}/{}/d1/f11'.format(tests.common.TEST_GCS_BUCKET,
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
                dst='gs://{}/{}/d1/f11'.format(tests.common.TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                no_clobber=False)

            blob_f11_not_updated = self.client._bucket.get_blob(
                '{}/d1/f11'.format(self.bucket_prefix))

            self.assertNotEqual(timestamp_f11, blob_f11_not_updated.updated)


class TestCPUploadNoCommonSetup(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())

    def tearDown(self):
        pass

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
                dst='gs://{}/{}/'.format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
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

    def test_upload_preserved_posix(self):
        with temppathlib.NamedTemporaryFile() as file:
            file.path.write_text(tests.common.TEST_GCS_BUCKET)

            url = "gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)

            self.client.cp(
                src=file.path.as_posix(), dst=url, preserve_posix=True)

            try:
                gcs_stat = self.client.stat(url=url)
                self.assertIsNotNone(gcs_stat)

                file_stat = file.path.stat()
                self.assertIsNotNone(file_stat)

                self.assertEqual(file_stat.st_size, gcs_stat.content_length)

                self.assertEqual(
                    datetime.datetime.utcfromtimestamp(
                        file_stat.st_mtime).replace(microsecond=0).timestamp(),
                    gcs_stat.file_mtime.timestamp())

                self.assertEqual(file_stat.st_uid, int(gcs_stat.posix_uid))
                self.assertEqual(file_stat.st_gid, int(gcs_stat.posix_gid))
                self.assertEqual(
                    oct(file_stat.st_mode)[-3:], gcs_stat.posix_mode)
            finally:
                tests.common.call_gsutil_rm(path=url, recursive=False)


if __name__ == '__main__':
    unittest.main()
