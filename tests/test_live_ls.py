#!/usr/bin/env python
"""Test gs-wrap ls live."""

# pylint: disable=missing-docstring

import tempfile
import unittest
import uuid

import google.api_core.exceptions

import gswrap
import tests.common


class TestLS(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        self.tmp_dir = tempfile.TemporaryDirectory()
        tests.common.gcs_test_setup(
            tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

    def tearDown(self):
        tests.common.gcs_test_teardown(prefix=self.bucket_prefix)
        self.tmp_dir.cleanup()

    def test_gsutil_vs_gswrap_ls_non_recursive(self):
        # yapf: disable
        test_cases = [
            "gs://{}".format(tests.common.TEST_GCS_BUCKET),
            "gs://{}/".format(tests.common.TEST_GCS_BUCKET),
            "gs://{}/{}".format(tests.common.TEST_GCS_BUCKET,
                                self.bucket_prefix),
            "gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                 self.bucket_prefix),
            "gs://{}/{}/d1".format(tests.common.TEST_GCS_BUCKET,
                                   self.bucket_prefix),
            "gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                    self.bucket_prefix),
            "gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)
        ]
        # yapf: enable
        for test_case in test_cases:
            list_gsutil = tests.common.call_gsutil_ls(path=test_case)
            list_gcs = self.client.ls(url=test_case, recursive=False)

            self.assertListEqual(list_gsutil, list_gcs)

    def test_gsutil_vs_gswrap_ls_non_recursive_check_raises(self):
        # yapf: disable
        test_cases = [
            "gs://{}".format(tests.common.TEST_GCS_BUCKET_NO_ACCESS),
            "gs://{}".format(tests.common.TEST_GCS_BUCKET[:-1]),
        ]
        # yapf: enable
        for test_case in test_cases:
            self.assertRaises(
                RuntimeError, tests.common.call_gsutil_ls, path=test_case)
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                self.client.ls,
                url=test_case,
                recursive=False)

    def test_gsutil_vs_gswrap_ls_non_recursive_check_empty(self):
        # yapf: disable
        test_cases = [
            "gs://{}/{}".format(tests.common.TEST_GCS_BUCKET,
                                self.bucket_prefix[:-1]),
            "gs://{}/{}/d".format(tests.common.TEST_GCS_BUCKET,
                                  self.bucket_prefix),
        ]
        # yapf: enable
        for test_case in test_cases:
            self.assertRaises(
                RuntimeError, tests.common.call_gsutil_ls, path=test_case)
            self.assertEqual([], self.client.ls(url=test_case, recursive=False))

    def test_gsutil_vs_gswrap_ls_recursive(self):
        # yapf: disable
        test_cases = [
            "gs://{}/{}".format(tests.common.TEST_GCS_BUCKET,
                                self.bucket_prefix),
            "gs://{}/{}/".format(tests.common.TEST_GCS_BUCKET,
                                 self.bucket_prefix),
            "gs://{}/{}/d1".format(tests.common.TEST_GCS_BUCKET,
                                   self.bucket_prefix),
            "gs://{}/{}/d1/".format(tests.common.TEST_GCS_BUCKET,
                                    self.bucket_prefix),
            "gs://{}/{}/d1/f11".format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix)
        ]
        # yapf: enable
        for test_case in test_cases:
            list_gsutil = tests.common.call_gsutil_ls(
                path=test_case, recursive=True)
            list_gcs = self.client.ls(url=test_case, recursive=True)
            # order of 'ls -r' is different
            self.assertListEqual(sorted(list_gsutil), sorted(list_gcs))

    def test_gsutil_vs_gswrap_ls_recursive_check_raises(self):
        # yapf: disable
        test_cases = [
            "gs://bucket-inexistent",  # google.api_core.exceptions.NotFound
            "gs://{}".format(tests.common.TEST_GCS_BUCKET_NO_ACCESS),
            # google.api_core.exceptions.Forbidden
            "gs://{}".format(tests.common.TEST_GCS_BUCKET[:-1]),
            # google.api_core.exceptions.NotFound
        ]
        # yapf: enable
        for test_case in test_cases:
            self.assertRaises(
                RuntimeError,
                tests.common.call_gsutil_ls,
                path=test_case,
                recursive=True)
            self.assertRaises(
                google.api_core.exceptions.GoogleAPIError,
                self.client.ls,
                url=test_case,
                recursive=True)

    def test_gsutil_vs_gswrap_ls_recursive_check_empty(self):
        # yapf: disable
        test_cases = [
            "gs://{}/{}".format(tests.common.TEST_GCS_BUCKET,
                                self.bucket_prefix[:-1]),
            "gs://{}/{}/d".format(tests.common.TEST_GCS_BUCKET,
                                  self.bucket_prefix),
        ]
        # yapf: enable
        for test_case in test_cases:
            self.assertRaises(
                RuntimeError,
                tests.common.call_gsutil_ls,
                path=test_case,
                recursive=True)
            self.assertEqual([], self.client.ls(url=test_case, recursive=True))

    def test_long_ls_nonrecursive(self):
        entries = self.client.long_ls(
            url=('gs://{}/{}/d1/'.format(tests.common.TEST_GCS_BUCKET, self.
                                         bucket_prefix)))

        self.assertEqual(
            'gs://{}/{}/d1/f11'.format(tests.common.TEST_GCS_BUCKET,
                                       self.bucket_prefix), entries[0][0])

        self.assertEqual(
            len(tests.common.GCS_FILE_CONTENT.encode('utf-8')),
            entries[0][1].content_length)

        self.assertTrue(entries[0][1].update_time is not None)

        self.assertEqual(
            'gs://{}/{}/d1/d11/'.format(tests.common.TEST_GCS_BUCKET,
                                        self.bucket_prefix), entries[1][0])

        self.assertTrue(entries[1][1] is None)

    def test_long_ls_recursive(self):
        entries = self.client.long_ls(
            url=('gs://{}/{}/d1/'.format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix)),
            recursive=True)

        urls = self.client.ls(
            url=('gs://{}/{}/d1/'.format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix)),
            recursive=True)

        for url, entry in zip(urls, entries):

            self.assertEqual(url, entry[0])

            self.assertEqual(
                len(tests.common.GCS_FILE_CONTENT.encode('utf-8')),
                entry[1].content_length)

            self.assertTrue(entry[1].update_time is not None)


if __name__ == '__main__':
    unittest.main()
