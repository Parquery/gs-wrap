#!/usr/bin/env python
"""Test gs-wrap."""

# pylint: disable=missing-docstring
# pylint: disable=protected-access

import unittest

import gswrap


class TestGCSPathlib(unittest.TestCase):
    def test_convert_to_posix(self):
        # pylint: disable=invalid-name
        local_file_pth = gswrap._GCSPathlib(path='/home/user/file')
        local_dir_pth = gswrap._GCSPathlib(path='/home/user/dir')
        gcs_prefix_no_trailing_backslash = gswrap._GCSPathlib(
            path='dir-in-bucket/sub-dir')
        gcs_prefix_trailing_backslash = gswrap._GCSPathlib(
            path='dir-in-bucket/sub-dir/')
        gcs_prefix_multiple_trailing_backslash = gswrap._GCSPathlib(
            path='dir-in-bucket/sub-dir///')
        local_cwd = gswrap._GCSPathlib(path='')
        gcs_cwd = gswrap._GCSPathlib(path='')
        local_root = gswrap._GCSPathlib(path='/')
        gcs_bucket_root = gswrap._GCSPathlib(path='/')

        self.assertFalse(local_file_pth._had_trailing_backslash)
        self.assertFalse(local_dir_pth._had_trailing_backslash)
        self.assertFalse(
            gcs_prefix_no_trailing_backslash._had_trailing_backslash)
        self.assertTrue(gcs_prefix_trailing_backslash._had_trailing_backslash)
        self.assertTrue(
            gcs_prefix_multiple_trailing_backslash._had_trailing_backslash)
        self.assertFalse(local_cwd._had_trailing_backslash)
        self.assertFalse(gcs_cwd._had_trailing_backslash)
        self.assertTrue(local_root._had_trailing_backslash)
        self.assertTrue(gcs_bucket_root._had_trailing_backslash)

        self.assertEqual('/home/user/file',
                         local_file_pth._convert_to_posix(is_cloud_url=False))
        self.assertEqual('/home/user/dir',
                         local_dir_pth._convert_to_posix(is_cloud_url=False))
        self.assertEqual(
            '/home/user/dir/',
            local_dir_pth._convert_to_posix(
                add_trailing_backslash=True, is_cloud_url=False))
        self.assertEqual(
            'dir-in-bucket/sub-dir',
            gcs_prefix_no_trailing_backslash._convert_to_posix(
                is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir/',
            gcs_prefix_no_trailing_backslash._convert_to_posix(
                add_trailing_backslash=True, is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir',
            gcs_prefix_trailing_backslash._convert_to_posix(is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir/',
            gcs_prefix_trailing_backslash._convert_to_posix(
                add_trailing_backslash=True, is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir',
            gcs_prefix_multiple_trailing_backslash._convert_to_posix(
                is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir/',
            gcs_prefix_multiple_trailing_backslash._convert_to_posix(
                add_trailing_backslash=True, is_cloud_url=True))
        self.assertEqual('.', local_cwd._convert_to_posix(is_cloud_url=False))
        self.assertEqual('', gcs_cwd._convert_to_posix(is_cloud_url=True))
        self.assertEqual('/', local_root._convert_to_posix(is_cloud_url=False))
        self.assertEqual('',
                         gcs_bucket_root._convert_to_posix(is_cloud_url=True))

    def test_start_with_backslash(self):
        absolut_pth = gswrap._GCSPathlib(path='/home/user/dir')
        not_absolut_pth = gswrap._GCSPathlib('dir/subdir')

        self.assertTrue(absolut_pth._starts_with_backslash())
        self.assertFalse(not_absolut_pth._starts_with_backslash())


class TestUniformPath(unittest.TestCase):
    def test_expected_structure(self):
        local_pth = gswrap._UniformPath(res_loc='/home/user/dir/sub-dir')
        gs_path = gswrap._UniformPath(
            res_loc='gs://bucket/folder-in-bucket/sub-dir')

        self.assertFalse(local_pth.is_cloud_url)
        self.assertEqual('', local_pth.bucket)
        self.assertEqual('/home/user/dir/sub-dir',
                         local_pth.prefix._convert_to_posix())

        self.assertTrue(gs_path.is_cloud_url)
        self.assertEqual('bucket', gs_path.bucket)
        self.assertEqual(
            'folder-in-bucket/sub-dir',
            gs_path.prefix._convert_to_posix(remove_leading_backslash=True))


class TestGSwrapFunctions(unittest.TestCase):
    def test_contains_wildcard(self):
        no_wildcard = 'no wildcard here'
        asterisk = '*/somedir'
        questionmark = 'f?lder'
        self.assertFalse(gswrap._contains_wildcard(prefix=no_wildcard))
        self.assertTrue(gswrap._contains_wildcard(prefix=asterisk))
        self.assertTrue(gswrap._contains_wildcard(prefix=questionmark))
        double_asterisk = 'folder/**/another-folder'
        self.assertTrue(gswrap._contains_wildcard(prefix=double_asterisk))

    def test_gcs_url_decomposition(self):
        bucket = 'parquery-sandbox'
        prefix = 'gsutil_playground/d1'
        link = 'gs://' + bucket + '/' + prefix
        url = gswrap._UniformPath(res_loc=link)

        self.assertEqual(bucket, url.bucket)
        self.assertEqual(
            prefix,
            url.prefix._convert_to_posix(
                add_trailing_backslash=url.prefix._had_trailing_backslash,
                remove_leading_backslash=True,
                is_cloud_url=True))
        self.assertTrue(url.is_cloud_url)

    def test_local_url_decomposition(self):
        bucket = ''
        prefix = '/user/home/'
        link = prefix
        url = gswrap._UniformPath(res_loc=link)

        self.assertEqual(bucket, url.bucket)
        self.assertEqual(
            prefix,
            url.prefix._convert_to_posix(
                add_trailing_backslash=url.prefix._had_trailing_backslash,
                is_cloud_url=False))
        self.assertFalse(url.is_cloud_url)


if __name__ == '__main__':
    unittest.main()
