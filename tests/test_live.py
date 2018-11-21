#!/usr/bin/env python
"""Test gs-wrap live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

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


class TestCreateRemove(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        self.tmp_dir = tempfile.TemporaryDirectory()
        tests.common.gcs_test_setup(
            tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

    def tearDown(self):
        tests.common.gcs_test_teardown(prefix=self.bucket_prefix)
        self.tmp_dir.cleanup()

    def test_remove_blob(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir:
            tmp_folder = local_tmpdir.path / str(uuid.uuid4())
            tmp_folder.mkdir()
            tmp_path = tmp_folder / 'file'
            tmp_path.write_text('hello')

            self.client.cp(
                src=local_tmpdir.path.as_posix(),
                dst='gs://{}/{}/'.format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
                recursive=True)

            parent_path = local_tmpdir.path.parent
            parent = local_tmpdir.path.as_posix().replace(
                parent_path.as_posix(), "", 1)
            if parent.startswith('/'):
                parent = parent[1:]

            files = self.client.ls(
                url='gs://{}/{}/{}'.format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix, parent),
                recursive=True)

            self.assertEqual(1, len(files), "More or less blobs found.")

            self.client.rm(
                url='gs://{}/{}/{}'.format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix, parent),
                recursive=True)

            self.assertRaises(
                RuntimeError,
                tests.common.call_gsutil_ls,
                path='gs://{}/{}/{}'.format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix, parent),
                recursive=True)

    def test_gsutil_vs_gswrap_remove_recursive(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            "gs://{}/{}/d1/d11/f111".format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix),
            "gs://{}/{}/d1/d11".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix),
            "gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                    self.bucket_prefix),
        ]
        # yapf: enable

        for test_case in test_cases:
            tests.common.gcs_test_setup(
                tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)
            self.client.rm(url=test_case, recursive=True)
            list_gcs = tests.common.call_gsutil_ls(
                path="gs://{}/{}".format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
                recursive=True)

            tests.common.gcs_test_setup(
                tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)
            tests.common.call_gsutil_rm(path=test_case, recursive=True)
            list_gsutil = tests.common.call_gsutil_ls(
                path="gs://{}/{}".format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
                recursive=True)

            self.assertListEqual(sorted(list_gsutil), sorted(list_gcs))

    def test_gsutil_vs_gswrap_remove_recursive_check_raises(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            "gs://{}/{}/d".format(tests.common.TEST_GCS_BUCKET,
                                  self.bucket_prefix),
            "gs://{}/{}/d/".format(tests.common.TEST_GCS_BUCKET,
                                   self.bucket_prefix),

        ]
        # yapf: enable

        for test_case in test_cases:
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                self.client.rm,
                url=test_case,
                recursive=True)

            self.assertRaises(
                subprocess.CalledProcessError,
                tests.common.call_gsutil_rm,
                path=test_case,
                recursive=True)

    def test_remove_non_recursive(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_case = "gs://{}/{}/d3/d31/d312/f3131".format(
            tests.common.TEST_GCS_BUCKET, self.bucket_prefix)
        # yapf: enable

        self.client.rm(url=test_case, recursive=False)
        list_gcs = tests.common.call_gsutil_ls(
            path="gs://{}/{}/d3/d31/d312/".format(tests.common.TEST_GCS_BUCKET,
                                                  self.bucket_prefix),
            recursive=False)

        self.assertListEqual([
            'gs://{}/{}/d3/d31/d312/f3132'.format(tests.common.TEST_GCS_BUCKET,
                                                  self.bucket_prefix)
        ], list_gcs)

    def test_gsutil_vs_gswrap_remove_non_recursive_check_raises(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            "gs://{}/{}/d1/d11".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix),
            "gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                    self.bucket_prefix),
            "gs://{}/{}/d".format(tests.common.TEST_GCS_BUCKET,
                                  self.bucket_prefix),
            "gs://{}/{}/d/".format(tests.common.TEST_GCS_BUCKET,
                                   self.bucket_prefix),
        ]
        # yapf: enable
        for test_case in test_cases:
            self.assertRaises(
                ValueError, self.client.rm, url=test_case, recursive=False)

            self.assertRaises(
                subprocess.CalledProcessError,
                tests.common.call_gsutil_rm,
                path=test_case,
                recursive=False)


class TestCPRemote(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        self.tmp_dir = tempfile.TemporaryDirectory()
        tests.common.gcs_test_setup(
            tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

    def tearDown(self):
        tests.common.gcs_test_teardown(prefix=self.bucket_prefix)
        self.tmp_dir.cleanup()

    def test_copy_file_to_file_in_same_bucket(self):
        src = 'gs://{}/{}/d1/f11'.format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix)
        dst = 'gs://{}/{}/ftest'.format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix)

        self.client.cp(src=src, dst=dst, recursive=False)

        src_blob = self.client._bucket.get_blob(
            blob_name="{}/d1/f11".format(self.bucket_prefix))
        src_text = src_blob.download_as_string()

        dst_blob = self.client._bucket.get_blob(
            blob_name="{}/ftest".format(self.bucket_prefix))
        dst_text = dst_blob.download_as_string()

        self.assertEqual(src_text, dst_text)

    def test_copy_folder_in_same_bucket(self):
        src = 'gs://{}/{}/d1/'.format(tests.common.TEST_GCS_BUCKET,
                                      self.bucket_prefix)
        dst = 'gs://{}/{}/dtest1/'.format(tests.common.TEST_GCS_BUCKET,
                                          self.bucket_prefix)

        self.client.cp(src=src, dst=dst, recursive=True)

        src_list = self.client.ls(url=src, recursive=True)
        dst_list = self.client.ls(url=dst, recursive=True)

        for src_file, dst_file in zip(src_list, dst_list):
            src_file = src_file.replace(src, dst + 'd1/')
            self.assertEqual(src_file, dst_file)

    def test_copy_files_in_same_bucket(self):
        src = 'gs://{}/{}/d1/'.format(tests.common.TEST_GCS_BUCKET,
                                      self.bucket_prefix)
        dst = 'gs://{}/{}/dtest1'.format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix)

        self.client.cp(src=src, dst=dst, recursive=True)

        src_list = self.client.ls(url=src, recursive=True)
        dst_list = self.client.ls(url=dst, recursive=True)

        for src_file, dst_file in zip(src_list, dst_list):
            src_file = src_file.replace(src, dst + '/')
            self.assertEqual(src_file, dst_file)

    def test_gsutil_vs_gswrap_copy_recursive(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            ["gs://{}/{}/d1".format(tests.common.TEST_GCS_BUCKET,
                                    self.bucket_prefix),
             "gs://{}/{}/dtest/".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix)],
            ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                     self.bucket_prefix),
             "gs://{}/{}/dtest/".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix)],
            ["gs://{}/{}/d1".format(tests.common.TEST_GCS_BUCKET,
                                    self.bucket_prefix),
             "gs://{}/{}/dtest".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)],
            ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                     self.bucket_prefix),
             "gs://{}/{}/dtest".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)],
            ["gs://{}/{}/d3/d31/d311/f3111".format(tests.common.TEST_GCS_BUCKET,
                                                   self.bucket_prefix),
             "gs://{}/{}/ftest".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)],
            ["gs://{}/{}/d3/d31/d311/f3111".format(tests.common.TEST_GCS_BUCKET,
                                                   self.bucket_prefix),
             "gs://{}/{}/ftest/".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix)],
            ["gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix),
             "gs://{}/{}/ftest".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)],
            ["gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix),
             "gs://{}/{}/ftest/".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix)],
        ]
        # yapf: enable

        gsutil_ls_set = set()
        gcs_ls_set = set()

        ls_path = "gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)

        for test_case in test_cases:
            self.client.cp(src=test_case[0], dst=test_case[1], recursive=True)
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

    def test_gsutil_vs_gswrap_copy_non_recursive(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            ["gs://{}/{}/d3/d31/d311/f3111".format(tests.common.TEST_GCS_BUCKET,
                                                   self.bucket_prefix),
             "gs://{}/{}/ftest".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)],
            ["gs://{}/{}/d3/d31/d311/f3111".format(tests.common.TEST_GCS_BUCKET,
                                                   self.bucket_prefix),
             "gs://{}/{}/ftest/".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix)],
            ["gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix),
             "gs://{}/{}/ftest".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)],
            ["gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix),
             "gs://{}/{}/ftest/".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix)],
        ]
        # yapf: enable

        gsutil_ls_set = set()
        gcs_ls_set = set()

        ls_path = "gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)

        for test_case in test_cases:
            self.client.cp(src=test_case[0], dst=test_case[1], recursive=False)
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

    def test_gsutil_vs_gswrap_copy_non_recursive_check_raises(self):  # pylint: disable=invalid-name
        # yapf: disable
        test_cases = [
            ["gs://{}/{}/d1".format(tests.common.TEST_GCS_BUCKET,
                                    self.bucket_prefix),
             "gs://{}/{}/dtest/".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix)],
            ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                     self.bucket_prefix),
             "gs://{}/{}/dtest/".format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix)],
            ["gs://{}/{}/d1".format(tests.common.TEST_GCS_BUCKET,
                                    self.bucket_prefix),
             "gs://{}/{}/dtest".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)],
            ["gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                     self.bucket_prefix),
             "gs://{}/{}/dtest".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)],
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

    def test_cp_no_clobber(self):
        # yapf: disable
        test_case = [
            "gs://{}/{}/d1/d11/f111".format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix)
            ,
            "gs://{}/{}/play/d2/ff".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)
        ]
        # yapf: enable

        path_f111 = gswrap.classifier(res_loc=test_case[0])
        path_ff = gswrap.classifier(res_loc=test_case[1])
        blob_f11 = self.client._bucket.get_blob(path_f111.prefix)
        blob_ff = self.client._bucket.get_blob(path_ff.prefix)

        timestamp_f11 = blob_f11.updated
        timestamp_ff = blob_ff.updated

        self.client.cp(src=test_case[0], dst=test_case[1], no_clobber=True)

        blob_f11_not_updated = self.client._bucket.get_blob(path_f111.prefix)
        blob_ff_not_updated = self.client._bucket.get_blob(path_ff.prefix)

        timestamp_f11_not_updated = blob_f11_not_updated.updated
        timestamp_ff_not_updated = blob_ff_not_updated.updated

        self.assertEqual(timestamp_f11, timestamp_f11_not_updated)
        self.assertEqual(timestamp_ff, timestamp_ff_not_updated)

    def test_cp_clobber(self):
        # yapf: disable
        test_case = [
            "gs://{}/{}/d1/d11/f111".format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix)
            ,
            "gs://{}/{}/play/d2/ff".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)
        ]
        # yapf: enable

        path_f111 = gswrap.classifier(res_loc=test_case[0])
        path_ff = gswrap.classifier(res_loc=test_case[1])
        blob_f11 = self.client._bucket.get_blob(path_f111.prefix)
        blob_ff = self.client._bucket.get_blob(path_ff.prefix)

        timestamp_f11 = blob_f11.updated
        timestamp_ff = blob_ff.updated

        self.client.cp(src=test_case[0], dst=test_case[1], no_clobber=False)

        blob_f11_not_updated = self.client._bucket.get_blob(path_f111.prefix)
        blob_ff_updated = self.client._bucket.get_blob(path_ff.prefix)

        timestamp_f11_not_updated = blob_f11_not_updated.updated
        timestamp_ff_updated = blob_ff_updated.updated

        self.assertEqual(timestamp_f11, timestamp_f11_not_updated)
        self.assertNotEqual(timestamp_ff, timestamp_ff_updated)


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


class TestCPDownload(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
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


class TestCPLocal(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.local_dir = pathlib.Path(self.tmp_dir.name) / str(uuid.uuid4())
        self.local_dir.mkdir()

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_gsutil_vs_gswrap_local_cp_file(self):  # pylint: disable=invalid-name
        local_file = self.local_dir / 'local-file'
        local_file.write_text('hello')

        for option in [True, False]:

            dst = self.local_dir / "dst_{}".format(option)

            self.client.cp(
                src=local_file.as_posix(), dst=dst.as_posix(), recursive=False)

            self.assertEqual("hello", dst.read_text())

    def test_gsutil_vs_gswrap_local_cp_dir(self):  # pylint: disable=invalid-name
        src_dir = self.local_dir / "src"
        src_dir.mkdir()
        local_file = src_dir / 'local-file'
        local_file.write_text('hello')

        dst_gsutil = self.local_dir / "dst_gsutil"
        dst_gswrap = self.local_dir / "dst_gswrap"

        self.client.cp(
            src=src_dir.as_posix(), dst=dst_gswrap.as_posix(), recursive=True)

        tests.common.call_gsutil_cp(
            src=src_dir.as_posix(), dst=dst_gsutil.as_posix(), recursive=True)

        for gsutil_file, gswrap_file in zip(
                tests.common.ls_local(dst_gsutil.as_posix()),
                tests.common.ls_local(dst_gswrap.as_posix())):

            self.assertEqual(
                pathlib.Path(gsutil_file).relative_to(dst_gsutil),
                pathlib.Path(gswrap_file).relative_to(dst_gswrap))

    def test_gsutil_vs_gswrap_local_cp_check_raises(self):  # pylint: disable=invalid-name
        local_path = pathlib.Path(self.tmp_dir.name)
        local_file = local_path / 'local-file'
        local_file.write_text('hello')
        file1 = self.local_dir / "file1"
        file1.write_text('hello')
        dst_dir = local_path / 'dst-dir'
        dst_dir.mkdir()

        test_case = [self.local_dir.as_posix(), dst_dir.as_posix(), False]

        self.assertRaises(
            subprocess.CalledProcessError,
            tests.common.call_gsutil_cp,
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
