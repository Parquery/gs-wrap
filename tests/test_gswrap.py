#!/usr/bin/env python
"""Test gs-wrap."""

# pylint: disable=missing-docstring
# pylint: disable=protected-access

import unittest

import gswrap


class TestGCSPathlib(unittest.TestCase):
    def test_path_as_posix(self):
        local_file_pth = gswrap._GCSPathlib(path='/home/user/file')
        local_dir_pth = gswrap._GCSPathlib(path='/home/user/dir')
        local_cwd = gswrap._GCSPathlib(path='')
        local_root = gswrap._GCSPathlib(path='/')

        # check backslash
        self.assertFalse(local_file_pth.had_trailing_backslash)
        self.assertFalse(local_dir_pth.had_trailing_backslash)
        self.assertFalse(local_cwd.had_trailing_backslash)
        self.assertTrue(local_root.had_trailing_backslash)

        # check posix representation
        self.assertEqual('/home/user/file',
                         local_file_pth.as_posix(is_cloud_url=False))
        self.assertEqual('/home/user/dir',
                         local_dir_pth.as_posix(is_cloud_url=False))
        self.assertEqual(
            '/home/user/dir/',
            local_dir_pth.as_posix(
                add_trailing_backslash=True, is_cloud_url=False))
        self.assertEqual('.', local_cwd.as_posix(is_cloud_url=False))
        self.assertEqual('/', local_root.as_posix(is_cloud_url=False))

    def test_gcs_url_as_posix(self):
        gcs_prefix_no_trailing_backslash = gswrap._GCSPathlib(
            path='dir-in-bucket/sub-dir')
        gcs_prefix_trailing_backslash = gswrap._GCSPathlib(
            path='dir-in-bucket/sub-dir/')
        gcs_prefix_multiple_trailing_backslash = gswrap._GCSPathlib(
            path='dir-in-bucket/sub-dir///')
        gcs_cwd = gswrap._GCSPathlib(path='')
        gcs_bucket_root = gswrap._GCSPathlib(path='/')

        # check backslash
        self.assertFalse(
            gcs_prefix_no_trailing_backslash.had_trailing_backslash)
        self.assertTrue(gcs_prefix_trailing_backslash.had_trailing_backslash)
        self.assertTrue(
            gcs_prefix_multiple_trailing_backslash.had_trailing_backslash)
        self.assertFalse(gcs_cwd.had_trailing_backslash)
        self.assertTrue(gcs_bucket_root.had_trailing_backslash)

        # check posix representation
        self.assertEqual(
            'dir-in-bucket/sub-dir',
            gcs_prefix_no_trailing_backslash.as_posix(is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir/',
            gcs_prefix_no_trailing_backslash.as_posix(
                add_trailing_backslash=True, is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir',
            gcs_prefix_trailing_backslash.as_posix(is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir/',
            gcs_prefix_trailing_backslash.as_posix(
                add_trailing_backslash=True, is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir',
            gcs_prefix_multiple_trailing_backslash.as_posix(is_cloud_url=True))
        self.assertEqual(
            'dir-in-bucket/sub-dir/',
            gcs_prefix_multiple_trailing_backslash.as_posix(
                add_trailing_backslash=True, is_cloud_url=True))
        self.assertEqual('', gcs_cwd.as_posix(is_cloud_url=True))
        self.assertEqual('', gcs_bucket_root.as_posix(is_cloud_url=True))

    def test_start_with_backslash(self):
        absolute_pth = gswrap._GCSPathlib(path='/home/user/dir')
        not_absolute_pth = gswrap._GCSPathlib('dir/subdir')

        self.assertTrue(absolute_pth.starts_with_backslash())
        self.assertFalse(not_absolute_pth.starts_with_backslash())


class TestUniformPath(unittest.TestCase):
    def test_expected_structure(self):
        local_pth = gswrap._UniformPath(res_loc='/home/user/dir/sub-dir')
        gs_path = gswrap._UniformPath(
            res_loc='gs://bucket/folder-in-bucket/sub-dir')

        self.assertFalse(local_pth.is_cloud_url)
        self.assertEqual('', local_pth.bucket)
        self.assertEqual('/home/user/dir/sub-dir', local_pth.prefix.as_posix())

        self.assertTrue(gs_path.is_cloud_url)
        self.assertEqual('bucket', gs_path.bucket)
        self.assertEqual('folder-in-bucket/sub-dir',
                         gs_path.prefix.as_posix(remove_leading_backslash=True))


class TestGSwrapFunctions(unittest.TestCase):
    def test_contains_wildcard(self):
        no_wildcard = 'no wildcard here'
        asterisk = '*/somedir'
        questionmark = 'f?lder'
        double_asterisk = 'folder/**/another-folder'

        self.assertFalse(gswrap._contains_wildcard(prefix=no_wildcard))
        self.assertTrue(gswrap._contains_wildcard(prefix=asterisk))
        self.assertTrue(gswrap._contains_wildcard(prefix=questionmark))
        self.assertTrue(gswrap._contains_wildcard(prefix=double_asterisk))

    def test_gcs_url_decomposition(self):
        bucket = 'your-bucket'
        prefix = 'your-dir/sub-dir'
        link = 'gs://' + bucket + '/' + prefix
        url = gswrap._UniformPath(res_loc=link)

        self.assertEqual(bucket, url.bucket)
        self.assertEqual(
            prefix,
            url.prefix.as_posix(
                add_trailing_backslash=url.prefix.had_trailing_backslash,
                remove_leading_backslash=True,
                is_cloud_url=True))
        self.assertTrue(url.is_cloud_url)

    def test_local_url_decomposition(self):
        bucket = ''
        prefix = '/home/user/'
        link = prefix
        url = gswrap._UniformPath(res_loc=link)

        self.assertEqual(bucket, url.bucket)
        self.assertEqual(
            prefix,
            url.prefix.as_posix(
                add_trailing_backslash=url.prefix.had_trailing_backslash,
                is_cloud_url=False))
        self.assertFalse(url.is_cloud_url)


if __name__ == '__main__':
    unittest.main()
