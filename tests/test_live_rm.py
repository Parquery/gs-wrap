#!/usr/bin/env python
"""Test gs-wrap rm live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

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


if __name__ == '__main__':
    unittest.main()
