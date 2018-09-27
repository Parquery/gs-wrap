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
from typing import Sequence, Tuple  # pylint: disable=unused-import

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
        gcs_bucket = 'gs://{}'.format(tests.common.TEST_GCS_BUCKET)
        prefix = self.bucket_prefix
        # yapf: disable
        test_cases = [
            (
                '{}/{}/d1/'.format(gcs_bucket, prefix),
                '{}/{}/d1-m2many'.format(gcs_bucket, prefix)
            ),
            (
                '{}/{}/d1/f11'.format(gcs_bucket, prefix),
                '{}/{}/d1-m2many/files/f11'.format(gcs_bucket, prefix)
            )
        ]  # type:Sequence[Tuple[str, pathlib.Path]]
        # yapf: enable

        self.client.cp_many_to_many(srcs_dsts=test_cases, recursive=True)

        # yapf: disable
        self.assertEqual(4, len(tests.common.call_gsutil_ls(
            path='{}/{}/d1-m2many'.format(gcs_bucket, prefix), recursive=True)))
        # yapf: enable

    def test_cp_download_many_to_many(self):
        gcs_bucket = 'gs://{}'.format(tests.common.TEST_GCS_BUCKET)
        prefix = self.bucket_prefix
        tmp_dir = self.tmp_dir.name
        # yapf: disable
        test_cases = [
            (
                '{}/{}/d1/'.format(gcs_bucket, prefix),
                pathlib.Path('{}/{}/d1-m2many'.format(tmp_dir, prefix))),
            (
                '{}/{}/d1/f11'.format(gcs_bucket, prefix),
                pathlib.Path('{}/{}/d1-m2many/files/f11'.format(tmp_dir, prefix)
                             ))]  # type:Sequence[Tuple[str, pathlib.Path]]
        # yapf: enable

        self.client.cp_many_to_many(srcs_dsts=test_cases, recursive=True)

        # yapf: disable
        self.assertEqual(4, len(tests.common.ls_local(
            path='{}/{}/d1-m2many'.format(self.tmp_dir.name, prefix))))
        # yapf: enable


if __name__ == '__main__':
    unittest.main()
