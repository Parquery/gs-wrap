#!/usr/bin/env python
"""Test gs-wrap cp many to many live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

import pathlib
import tempfile
import unittest
import uuid
from typing import Sequence, Tuple, Union  # pylint: disable=unused-import

import gswrap
import tests.common


class TestCPManyToMany(unittest.TestCase):
    def setUp(self):
        self.client = gswrap.Client(bucket_name=tests.common.TEST_GCS_BUCKET)
        self.bucket_prefix = str(uuid.uuid4())
        self.tmp_dir = tempfile.TemporaryDirectory()
        tests.common.gcs_test_setup(
            tmp_dir_name=self.tmp_dir.name, prefix=self.bucket_prefix)

    def tearDown(self):
        tests.common.gcs_test_teardown(prefix=self.bucket_prefix)
        self.tmp_dir.cleanup()

    def test_cp_remote_many_to_many(self):
        test_cases = [('gs://{}/{}/d1/'.format(tests.common.TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                       'gs://{}/{}/d1-many-to-many'.format(
                           tests.common.TEST_GCS_BUCKET, self.bucket_prefix)),
                      ('gs://{}/{}/d1/f11'.format(tests.common.TEST_GCS_BUCKET,
                                                  self.bucket_prefix),
                       'gs://{}/{}/d1-many-to-many/files/f11'.format(
                           tests.common.TEST_GCS_BUCKET, self.bucket_prefix))] \
            # type:Sequence[Tuple[str, pathlib.Path]]

        self.client.cp_many_to_many(srcs_dsts=test_cases, recursive=True)

        self.assertEqual(
            4,
            len(
                tests.common.call_gsutil_ls(
                    path='gs://{}/{}/d1-many-to-many'.format(
                        tests.common.TEST_GCS_BUCKET, self.bucket_prefix),
                    recursive=True)))

    def test_cp_download_many_to_many(self):
        test_cases = [('gs://{}/{}/d1/'.format(tests.common.TEST_GCS_BUCKET,
                                               self.bucket_prefix),
                       pathlib.Path('{}/{}/d1-many-to-many'.format(
                           self.tmp_dir, self.bucket_prefix))),
                      ('gs://{}/{}/d1/f11'.format(tests.common.TEST_GCS_BUCKET,
                                                  self.bucket_prefix),
                       pathlib.Path('{}/{}/d1-many-to-many/files/f11'.format(
                           self.tmp_dir, self.bucket_prefix)))] \
            # type:Sequence[Tuple[str, pathlib.Path]]

        self.client.cp_many_to_many(srcs_dsts=test_cases, recursive=True)

        self.assertEqual(
            4,
            len(
                tests.common.ls_local(path='{}/{}/d1-many-to-many'.format(
                    self.tmp_dir, self.bucket_prefix))))


if __name__ == '__main__':
    unittest.main()
