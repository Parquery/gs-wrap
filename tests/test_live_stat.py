#!/usr/bin/env python
"""Test gs-wrap stat live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

import datetime
import subprocess
import unittest
import uuid

import temppathlib

import gswrap
import tests.common


class TestStat(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())

    def tearDown(self):
        pass

    def test_stat(self):
        with temppathlib.NamedTemporaryFile() as file:
            file.path.write_text(tests.common.GCS_FILE_CONTENT)

            url = "gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)

            subprocess.check_call(
                ["gsutil", "cp", "-P",
                 file.path.as_posix(), url])

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

    def test_same_md5(self):
        with temppathlib.NamedTemporaryFile() as file:
            file.path.write_text(tests.common.GCS_FILE_CONTENT)

            url = "gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)
            tests.common.call_gsutil_cp(
                src=file.path.as_posix(), dst=url, recursive=False)
            try:
                self.assertTrue(self.client.same_md5(path=file.path, url=url))
            finally:
                tests.common.call_gsutil_rm(path=url, recursive=False)

    def test_different_md5(self):
        with temppathlib.NamedTemporaryFile() as file:
            file.path.write_text(tests.common.GCS_FILE_CONTENT)

            url = "gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)
            tests.common.call_gsutil_cp(
                src=file.path.as_posix(), dst=url, recursive=False)

            file.path.write_text("write something more")
            try:
                self.assertFalse(self.client.same_md5(path=file.path, url=url))
            finally:
                tests.common.call_gsutil_rm(path=url, recursive=False)

    def test_md5_hexdigest(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            path = tmp_dir.path / 'file'
            path.write_text('calculated md5 hash of this content')

            another_path = tmp_dir.path / 'another-file'
            another_path.write_text(
                "my md5 has is also calculated, please don't change")

            url = "gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)
            another_url = "gs://{}/{}/another-file".format(
                tests.common.TEST_GCS_BUCKET, self.bucket_prefix)
            nonexisting_url = "gs://{}/{}/nonexisting-file".format(
                tests.common.TEST_GCS_BUCKET, self.bucket_prefix)

            tests.common.call_gsutil_cp(
                src=path.as_posix(), dst=url, recursive=False)
            tests.common.call_gsutil_cp(
                src=another_path.as_posix(), dst=another_url, recursive=False)

            try:
                self.assertTrue(
                    self.client.same_md5(path=path, url=url),
                    "Expected md5 to be the same, but they were different.")

                self.assertFalse(
                    self.client.same_md5(path=path, url=nonexisting_url),
                    "Expected md5 to be different when the object doesn't "
                    "exist, but they were same.")

                expected_md5_hexdigests = [
                    '5263a575f07b61be1023bc2fa09cc722',
                    'dfc9d887c31ba3c4489a5e290ab48c75'
                ]

                md5_hexdigests = self.client.md5_hexdigests(
                    urls=[url, another_url])

                self.assertListEqual(expected_md5_hexdigests, md5_hexdigests)
            finally:
                tests.common.call_gsutil_rm(
                    path="gs://{}/{}".format(tests.common.TEST_GCS_BUCKET,
                                             self.bucket_prefix),
                    recursive=True)

    def test_same_modtime(self):
        with temppathlib.NamedTemporaryFile() as file:
            file.path.touch()
            file.path.write_text(tests.common.GCS_FILE_CONTENT)
            url = "gs://{}/{}/file".format(tests.common.TEST_GCS_BUCKET,
                                           self.bucket_prefix)
            subprocess.check_call(
                ["gsutil", "cp", "-P",
                 file.path.as_posix(), url])
            try:
                self.assertTrue(
                    self.client.same_modtime(path=file.path, url=url))
            finally:
                tests.common.call_gsutil_rm(path=url, recursive=False)


if __name__ == '__main__':
    unittest.main()
