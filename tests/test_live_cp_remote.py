#!/usr/bin/env python
"""Test gs-wrap cp remote live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

import subprocess
import tempfile
import unittest
import uuid
from typing import Set

import google.api_core.exceptions

import gswrap
import tests.common


class TestCPRemote(unittest.TestCase):
    def setUp(self) -> None:
        self.client = gswrap.Client()
        self.client._change_bucket(tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        self.tmp_dir = tempfile.TemporaryDirectory()
        tests.common.gcs_test_setup(
            tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

    def tearDown(self) -> None:
        tests.common.gcs_test_teardown(prefix=self.bucket_prefix)
        self.tmp_dir.cleanup()

    def test_copy_file_to_file_in_same_bucket(self) -> None:
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

    def test_copy_folder_in_same_bucket(self) -> None:
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

    def test_copy_files_in_same_bucket(self) -> None:
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

    def test_gsutil_vs_gswrap_copy_recursive(self) -> None:  # pylint: disable=invalid-name
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

        gsutil_ls_set = set()  # type: Set[str]
        gcs_ls_set = set()  # type: Set[str]

        ls_path = "gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)

        for test_case in test_cases:
            self.client.cp(src=test_case[0], dst=test_case[1], recursive=True)
            gcs_paths = self.client.ls(url=ls_path, recursive=True)
            gcs_ls_set.union(gcs_paths)
            tests.common.call_gsutil_rm(
                path="gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                          self.bucket_prefix),
                recursive=True)
            tests.common.gcs_test_setup(
                tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

            tests.common.call_gsutil_cp(
                src=test_case[0], dst=test_case[1], recursive=True)
            gsutil_paths = self.client.ls(url=ls_path, recursive=True)
            gsutil_ls_set.union(gsutil_paths)
            tests.common.call_gsutil_rm(
                path="gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                          self.bucket_prefix),
                recursive=True)
            tests.common.gcs_test_setup(
                tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

            self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

        self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_copy_non_recursive(self) -> None:  # pylint: disable=invalid-name
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

        gsutil_ls_set = set()  # type: Set[str]
        gcs_ls_set = set()  # type: Set[str]

        ls_path = "gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)

        for test_case in test_cases:
            self.client.cp(src=test_case[0], dst=test_case[1], recursive=False)
            gcs_paths = self.client.ls(url=ls_path, recursive=True)
            gcs_ls_set.union(gcs_paths)
            tests.common.call_gsutil_rm(
                path="gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                          self.bucket_prefix),
                recursive=True)
            tests.common.gcs_test_setup(
                tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

            tests.common.call_gsutil_cp(
                src=test_case[0], dst=test_case[1], recursive=False)
            gsutil_paths = self.client.ls(url=ls_path, recursive=True)
            gsutil_ls_set.union(gsutil_paths)
            tests.common.call_gsutil_rm(
                path="gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                          self.bucket_prefix),
                recursive=True)
            tests.common.gcs_test_setup(
                tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

            self.assertListEqual(sorted(gsutil_paths), sorted(gcs_paths))

        self.assertListEqual(list(gsutil_ls_set), list(gcs_ls_set))

    def test_gsutil_vs_gswrap_copy_non_recursive_check_raises(self) -> None:  # pylint: disable=invalid-name
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

    def test_cp_no_clobber(self) -> None:
        # yapf: disable
        test_case = [
            "gs://{}/{}/d1/d11/f111".format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix)
            ,
            "gs://{}/{}/play/d2/ff".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)
        ]
        # yapf: enable

        path_f111 = gswrap.resource_type(res_loc=test_case[0])
        path_ff = gswrap.resource_type(res_loc=test_case[1])
        assert isinstance(path_f111, gswrap._GCSURL)
        assert isinstance(path_ff, gswrap._GCSURL)
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

    def test_cp_clobber(self) -> None:
        # yapf: disable
        test_case = [
            "gs://{}/{}/d1/d11/f111".format(tests.common.TEST_GCS_BUCKET,
                                            self.bucket_prefix)
            ,
            "gs://{}/{}/play/d2/ff".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)
        ]
        # yapf: enable

        path_f111 = gswrap.resource_type(res_loc=test_case[0])
        path_ff = gswrap.resource_type(res_loc=test_case[1])
        assert isinstance(path_f111, gswrap._GCSURL)
        assert isinstance(path_ff, gswrap._GCSURL)
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


if __name__ == '__main__':
    unittest.main()
