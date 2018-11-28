#!/usr/bin/env python
"""Test gswrap."""

# pylint: disable=missing-docstring
# pylint: disable=protected-access

import unittest

import gswrap


class TestGCSURL(unittest.TestCase):
    def test_expected_structure(self):
        gs_path = gswrap._GCSURL(
            bucket="bucket", prefix="folder-in-bucket/sub-dir")

        self.assertEqual('bucket', gs_path.bucket)
        self.assertEqual('folder-in-bucket/sub-dir', gs_path.prefix)


class TestGSwrapFunctions(unittest.TestCase):
    def test_contains_wildcard(self):
        no_wildcard = 'no wildcard here'
        asterisk = '*/somedir'
        questionmark = 'f?lder'
        double_asterisk = 'folder/**/another-folder'

        self.assertFalse(gswrap.contains_wildcard(prefix=no_wildcard))
        self.assertTrue(gswrap.contains_wildcard(prefix=asterisk))
        self.assertTrue(gswrap.contains_wildcard(prefix=questionmark))
        self.assertTrue(gswrap.contains_wildcard(prefix=double_asterisk))

    def test_gcs_url_decomposition(self):
        bucket = 'your-bucket'
        prefix = 'your-dir/sub-dir'
        link = 'gs://' + bucket + '/' + prefix
        url = gswrap.classify(res_loc=link)

        self.assertTrue(isinstance(url, gswrap._GCSURL))
        self.assertEqual(bucket, url.bucket)
        self.assertEqual(prefix, url.prefix)

    def test_local_url_decomposition(self):
        path = '/home/user/'
        url = gswrap.classify(res_loc=path)

        self.assertTrue(isinstance(url, str))
        self.assertEqual(path, url)


if __name__ == '__main__':
    unittest.main()
