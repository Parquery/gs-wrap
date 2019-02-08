#!/usr/bin/env python
"""Test gs-wrap cp local live."""

# pylint: disable=missing-docstring
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=expression-not-assigned

import pathlib
import subprocess
import tempfile
import unittest
import uuid

import temppathlib

import gswrap
import tests.common


class TestCPLocal(unittest.TestCase):
    def setUp(self) -> None:
        self.client = gswrap.Client()
        self.client._change_bucket(tests.common.TEST_GCS_BUCKET)
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.local_dir = pathlib.Path(self.tmp_dir.name) / str(uuid.uuid4())
        self.local_dir.mkdir()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_gsutil_vs_gswrap_local_cp_file(self) -> None:  # pylint: disable=invalid-name
        local_file = self.local_dir / 'local-file'
        local_file.write_text('hello')

        for option in [True, False]:

            dst = self.local_dir / "dst_{}".format(option)

            self.client.cp(
                src=local_file.as_posix(), dst=dst.as_posix(), recursive=False)

            self.assertEqual("hello", dst.read_text())

    def test_gsutil_vs_gswrap_local_cp_dir(self) -> None:  # pylint: disable=invalid-name
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

    def test_gsutil_vs_gswrap_local_cp_check_raises(self) -> None:  # pylint: disable=invalid-name
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

    def test_cp_local_no_clobber(self) -> None:
        with temppathlib.NamedTemporaryFile() as tmp_file1, \
                temppathlib.NamedTemporaryFile() as tmp_file2:
            tmp_file1.path.write_text("hello")
            tmp_file2.path.write_text("hello there")

            self.client.cp(
                src=tmp_file1.path.as_posix(),
                dst=tmp_file2.path.as_posix(),
                no_clobber=True)

            self.assertEqual("hello there", tmp_file2.path.read_text())

    def test_cp_local_clobber(self) -> None:
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
