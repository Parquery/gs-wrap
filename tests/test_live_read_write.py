#!/usr/bin/env python
"""Test gs-wrap read write live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

import unittest
import uuid

import temppathlib

import gswrap
import tests.common


class TestReadWrite(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())

    def tearDown(self):
        pass

    def test_read_bytes(self):
        with temppathlib.NamedTemporaryFile() as file:
            file.path.write_bytes(tests.common.GCS_FILE_CONTENT.encode('utf-8'))
            tests.common.call_gsutil_cp(
                src=file.path.as_posix(),
                dst="gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                             self.bucket_prefix),
                recursive=False)

            content = self.client.read_bytes(url="gs://{}/{}/file".format(
                tests.common.TEST_GCS_BUCKET, self.bucket_prefix))
            self.assertEqual(
                tests.common.GCS_FILE_CONTENT.encode('utf-8'), content)
            tests.common.call_gsutil_rm(path="gs://{}/{}/file".format(
                tests.common.TEST_GCS_BUCKET, self.bucket_prefix))

    def test_read_text(self):
        with temppathlib.NamedTemporaryFile() as file:
            file.path.write_text(tests.common.GCS_FILE_CONTENT)
            tests.common.call_gsutil_cp(
                src=file.path.as_posix(),
                dst="gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                             self.bucket_prefix),
                recursive=False)

            content = self.client.read_text(url="gs://{}/{}/file".format(
                tests.common.TEST_GCS_BUCKET, self.bucket_prefix))
            self.assertEqual(tests.common.GCS_FILE_CONTENT, content)
            tests.common.call_gsutil_rm(path="gs://{}/{}/file".format(
                tests.common.TEST_GCS_BUCKET, self.bucket_prefix))

    def test_write_bytes(self):
        self.client.write_bytes(
            url="gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                         self.bucket_prefix),
            data=b'hello')

        with temppathlib.NamedTemporaryFile() as file:
            tests.common.call_gsutil_cp(
                src="gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                             self.bucket_prefix),
                dst=file.path.as_posix(),
                recursive=False)
            content = file.path.read_bytes()

            self.assertEqual(b'hello', content)

        tests.common.call_gsutil_rm(
            path="gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                          self.bucket_prefix),
            recursive=False)

    def test_write_text(self):
        self.client.write_text(
            url="gs://{}/{}/utf-file".format(tests.common.TEST_GCS_BUCKET,
                                             self.bucket_prefix),
            text=tests.common.GCS_FILE_CONTENT,
            encoding='utf-8')
        self.client.write_text(
            url="gs://{}/{}/iso-file".format(tests.common.TEST_GCS_BUCKET,
                                             self.bucket_prefix),
            text=tests.common.GCS_FILE_CONTENT,
            encoding='iso-8859-1')

        with temppathlib.NamedTemporaryFile() as file:
            tests.common.call_gsutil_cp(
                src="gs://{}/{}/utf-file".format(tests.common.TEST_GCS_BUCKET,
                                                 self.bucket_prefix),
                dst=file.path.as_posix(),
                recursive=False)
            utf_content = file.path.read_text(encoding='utf-8')

            self.assertEqual(tests.common.GCS_FILE_CONTENT, utf_content)

            tests.common.call_gsutil_cp(
                src="gs://{}/{}/iso-file".format(tests.common.TEST_GCS_BUCKET,
                                                 self.bucket_prefix),
                dst=file.path.as_posix(),
                recursive=False)
            iso_content = file.path.read_text(encoding='iso-8859-1')

            self.assertEqual(tests.common.GCS_FILE_CONTENT, iso_content)

        tests.common.call_gsutil_rm(
            path="gs://{}/{}/utf-file".format(tests.common.TEST_GCS_BUCKET,
                                              self.bucket_prefix),
            recursive=False)
        tests.common.call_gsutil_rm(
            path="gs://{}/{}/iso-file".format(tests.common.TEST_GCS_BUCKET,
                                              self.bucket_prefix),
            recursive=False)


if __name__ == '__main__':
    unittest.main()
