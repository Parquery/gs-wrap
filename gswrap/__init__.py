#!/usr/bin/env python
"""Wrap Google Cloud Storage API for multi-threaded data manipulation."""

# pylint: disable=protected-access
# pylint: disable=too-many-lines

import base64
import concurrent.futures
import datetime
import hashlib
import os
import pathlib
import re
import shutil
import urllib.parse
from typing import List, Optional, Sequence, Tuple, Union

import google.api_core.exceptions
import google.api_core.page_iterator
import google.auth.credentials
import google.cloud.storage
import icontract


class _GCSURL:
    """
    Store Google Cloud Storage URL.

    :ivar bucket: name of the bucket
    :vartype bucket: str

    :ivar prefix: name of the prefix
    :vartype prefix: str
    """

    def __init__(self, bucket: str, prefix: str) -> None:
        """
        Initialize Google Cloud Storage url structure.

        :param bucket: name of the bucket
        :param prefix: name of the prefix
        """
        self.bucket = bucket

        # prefixes in google cloud storage never have a leading slash
        if prefix.startswith('/'):
            prefix = prefix[1:]

        self.prefix = prefix


def resource_type(res_loc: str) -> Union[_GCSURL, str]:
    """
    Determine resource type.

    >>> url = resource_type(res_loc='gs://your-bucket/some-dir/file')
    >>> isinstance(url, _GCSURL)
    True
    >>> url.bucket
    'your-bucket'
    >>> url.prefix
    'some-dir/file'

    >>> path = resource_type(res_loc='/home/user/work/file')
    >>> path
    '/home/user/work/file'
    >>> isinstance(path, str)
    True

    :param res_loc: resource location
    :return: class corresponding to the file/directory location
    """
    parsed_path = urllib.parse.urlparse(url=res_loc)

    if parsed_path.scheme == 'gs':
        return _GCSURL(bucket=parsed_path.netloc, prefix=parsed_path.path)

    if parsed_path.scheme == '' or parsed_path.scheme == 'file':
        return parsed_path.path

    raise google.api_core.exceptions.GoogleAPIError(
        "Unrecognized scheme '{}'.".format(parsed_path.scheme))


_WILDCARDS_RE = re.compile(r'(\*\*|\*|\?|\[[^]]+\])')


def contains_wildcard(prefix: str) -> bool:
    """
    Check if prefix contains any wildcards.

    >>> contains_wildcard(prefix='gs://your-bucket/some-dir/file')
    False
    >>> contains_wildcard(prefix='gs://your-bucket/*/file')
    True

    :param prefix: path to a file or a directory
    :return:
    """
    match_object = _WILDCARDS_RE.search(prefix)
    return match_object is not None


def _list_blobs(
        iterator: google.api_core.page_iterator.HTTPIterator) -> List[str]:
    """
    List files and directories for a given iterator in expected gsutil order.

    :param iterator: iterator returned from
    google.cloud.storage.bucket.Bucket.list_blobs
    :return: List of blobs and directories that were found by the iterator
    """
    # creating a list is necessary to populate iterator.prefixes
    # check out the following issue:
    # https://github.com/googleapis/google-cloud-python/issues/920

    blob_names = []  # type: List[str]
    for obj in iterator:
        blob_names.append(obj.name)

    subdirectories = iterator.prefixes
    subdirectories = sorted(subdirectories)
    for subdir in subdirectories:
        blob_names.append(subdir)

    return blob_names


def _rename_destination_blob(blob_name: str, src: _GCSURL,
                             dst: _GCSURL) -> pathlib.Path:
    """
    Rename destination blob name to achieve gsutil-like cp -r behaviour.

    Gsutil behavior for recursive copy commands on Google cloud depends on
    trailing slash.
    | e.g.    Existing blob on Google Cloud
    |        gs://your-bucket/your-dir/file

    |         gsutil cp -r gs://your-bucket/your-dir/ gs://your-bucket/copy-dir/
    |         gs://your-bucket/copy-dir/your-dir/file

    |         gsutil cp -r gs://your-bucket/your-dir/ gs://your-bucket/copy-dir
    |         gs://your-bucket/copy-dir/file

    :param blob_name: name of the blob which is copied
    :param src: source prefix from where the blob is copied
    :param dst: destination prefix to where the blob should be copied
    :return: new destination blob name
    """
    src_suffix = blob_name[len(src.prefix):]

    if src_suffix.startswith('/'):
        src_suffix = src_suffix[1:]

    if dst.prefix.endswith('/'):
        parent_path = pathlib.Path(src.prefix).parent
        src_prefix_parent = src.prefix.replace(parent_path.as_posix(), "", 1)
        if src_prefix_parent.startswith('/'):
            src_prefix_parent = src_prefix_parent[1:]
        return pathlib.Path(dst.prefix) / src_prefix_parent / src_suffix

    return pathlib.Path(dst.prefix) / src_suffix


class Stat:
    """
    Represent stat of an object in Google Storage.

    Times are given in UTC.

    :ivar creation_time: time when blob on google cloud storage was created.
        Not equal creation time of the local file.
    :vartype creation_time: Optional[datetime.datetime]

    :ivar update_time: time when blob on google cloud storage was last updated.
        Not equal modification time of the local file.
    :vartype update_time: Optional[datetime.datetime]

    :ivar storage_class: tells in what kind of storage data is stored.
        More information: https://cloud.google.com/storage/docs/storage-classes
    :vartype storage_class: Optional[str]

    :ivar content_length: size of the object
    :vartype content_length: Optional[int]

    :ivar file_mtime: modification time of the local file stored in the metadata
        of the blob. Only available when file was uploaded with preserve_posix.
    :vartype file_mtime: Optional[datetime.datetime]

    :ivar file_atime: last access time of the local file stored in the metadata
        of the blob. Only available when file was uploaded with preserve_posix.
    :vartype file_atime: Optional[datetime.datetime]

    :ivar posix_uid: user id of the owner of the local file stored in the
        metadata of the blob.
        Only available when file was uploaded with preserve_posix.
    :vartype posix_uid: Optional[str]

    :ivar posix_gid: group id of the owner of the local file stored in the
        metadata of the blob.
        Only available when file was uploaded with preserve_posix.
    :vartype posix_gid: Optional[str]

    :ivar posix_mode: inode protection mode of the local file stored in the
        metadata of the blob.
        Only available when file was uploaded with preserve_posix.
    :vartype posix_mode: Optional[str]

    :ivar crc32c: CRC32C checksum for this object.
    :vartype crc32c: Optional[bytes]

    :ivar md5: MD5 hash for this object.
    :vartype md5: Optional[bytes]
    """

    # pylint: disable=too-many-instance-attributes
    def __init__(self):
        """Initialize."""
        self.creation_time = None
        self.update_time = None
        self.storage_class = None
        self.content_length = None
        self.file_mtime = None
        self.file_atime = None
        self.posix_uid = None
        self.posix_gid = None
        self.posix_mode = None
        self.crc32c = None
        self.md5 = None


def _os_stat_to_blob_metadata(path: Union[str, pathlib.Path],
                              blob: google.cloud.storage.blob.Blob):
    """
    Store os.stat() information from local file in google cloud blob's metadata.

    :param path: of the local file of which stats are read
    :param blob: to which metadata is added
    :return:
    """
    path_str = path if isinstance(path, str) else path.as_posix()

    stats = os.stat(path_str)

    new_metadata = {
        'goog-reserved-file-atime': int(stats.st_atime),
        'goog-reserved-file-mtime': int(stats.st_mtime),
        'goog-reserved-posix-uid': stats.st_uid,
        'goog-reserved-posix-gid': stats.st_gid,
        # https://stackoverflow.com/a/5337329
        # os.stat('file').st_mode returns an int, but we like octal
        'goog-reserved-posix-mode': oct(stats.st_mode)[-3:]
    }
    blob.metadata = new_metadata
    blob.patch()


def _blob_metadata_to_os_stat(path: Union[str, pathlib.Path],
                              blob: google.cloud.storage.blob.Blob):
    """
    Store google cloud blob's metadata to os.stat() information of a local file.

    :param path: of the local file which stats are set
    :param blob: of which metadata is read
    :return:
    """
    path_str = path if isinstance(path, str) else path.as_posix()

    if 'goog-reserved-file-atime' in blob.metadata and \
            'goog-reserved-file-mtime' in blob.metadata:
        a_time = blob.metadata['goog-reserved-file-atime']
        m_time = blob.metadata['goog-reserved-file-mtime']
        os.utime(path_str, (int(a_time), int(m_time)))

    if 'goog-reserved-posix-uid' in blob.metadata:
        os.setuid(int(blob.metadata['goog-reserved-posix-uid']))

    if 'goog-reserved-posix-gid' in blob.metadata:
        os.setgid(int(blob.metadata['goog-reserved-posix-gid']))

    if 'goog-reserved-posix-mode' in blob.metadata:
        os.chmod(path_str, int(blob.metadata['goog-reserved-posix-mode'], 8))


def _upload_from_path(blob: google.cloud.storage.blob.Blob,
                      path: Union[str, pathlib.Path],
                      preserve_posix: bool = False):
    """
    Upload from path with the option to preserve POSIX attributes.

    :param blob: where file will be uploaded to
    :param path: path of the file to upload
    :param preserve_posix:
        if true then copy os.stat to blob metadata, else no metadata is created
    :return:
    """
    path_str = path if isinstance(path, str) else path.as_posix()
    blob.upload_from_filename(filename=path_str)

    if preserve_posix:
        _os_stat_to_blob_metadata(path=path_str, blob=blob)


def _download_to_path(blob: google.cloud.storage.blob.Blob,
                      path: Union[str, pathlib.Path],
                      preserve_posix: bool = False):
    """
    Download to path with the option to preserve POSIX attributes.

    :param blob: blob that will be downloaded
    :param path: path where blob will be downloaded to
    :param preserve_posix:
        if true then copy blob metadata to file stats, else os.stat will differ
    :return:
    """
    local_pth = path if isinstance(path, pathlib.Path) else pathlib.Path(path)

    parent = pathlib.Path(local_pth.parent)
    # exist_ok because another thread might be faster creating the directory
    parent.mkdir(parents=True, exist_ok=True)

    blob.download_to_filename(filename=local_pth.as_posix())

    if preserve_posix:
        _blob_metadata_to_os_stat(path=local_pth.as_posix(), blob=blob)


class Client:
    """Google Cloud Storage Client for simple usage of gsutil commands."""

    def __init__(self, project: Optional[str] = None) -> None:
        """
        Initialize.

        :param project:
            The Google Cloud Storage project which the client acts on behalf of.
            It will be passed when creating the internal client. If not passed,
            falls back to the default inferred from the locally authenticated
            Google Cloud SDK (http://cloud.google.com/sdk) environment. Each
            project needs a separate client. Operations between two different
            projects are not supported.
        """
        if project is not None:
            self._client = google.cloud.storage.Client(project=project)
        else:
            self._client = google.cloud.storage.Client()

        self._bucket = None  # type: google.cloud.storage.Bucket

    def _change_bucket(self, bucket_name: str) -> None:
        """
        Change active bucket.

        :param bucket_name: name of the bucket to activate
        """
        if self._bucket is None or bucket_name != self._bucket.name:
            self._bucket = self._client.get_bucket(bucket_name=bucket_name)

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    def ls(self, url: str, recursive: bool = False) -> List[str]:  # pylint: disable=invalid-name
        """
        List the files on Google Cloud Storage given the prefix.

        Functionality is the same as "gsutil ls (-r)" command. Except that no
        wildcards are allowed. For more information about "gsutil ls" check out:
        https://cloud.google.com/storage/docs/gsutil/commands/ls

        | existing blob at your storage:
        | gs://your-bucket/something

        | ls(url="gs://your-bucket/some", recursive=True)
        | will raise a GoogleAPI error because no url is matching

        | if there is another blob:
        | gs://your-bucket/some/blob

        | ls(url="gs://your-bucket/some", recursive=True)
        | will return this blob: "gs://your-bucket/some/blob"

        :param url: Google cloud storage URL
        :param recursive: List only direct subdirectories
        :return: List of Google cloud storage URLs according the given URL
        """
        ls_url = resource_type(res_loc=url)

        assert isinstance(ls_url, _GCSURL)

        blobs = self._ls(url=ls_url, recursive=recursive)

        blob_list = []
        for element in blobs:
            blob_list.append('gs://' + ls_url.bucket + '/' + element)

        return blob_list

    def _ls(self, url: _GCSURL, recursive: bool = False) -> List[str]:
        """
        List the files on Google Cloud Storage given the prefix.

        :param url: uniform google cloud url
        :param recursive:
            if True, list directories recursively
            if False, list only direct subdirectory
        :return: List of the blob names found by list_blobs using the given
                 prefix
        """
        if recursive:
            delimiter = ''
        else:
            delimiter = '/'

        self._change_bucket(bucket_name=url.bucket)
        is_not_blob = url.prefix == "" or self._bucket.get_blob(
            blob_name=url.prefix) is None

        # add trailing slash to limit search to this file/folder.
        if not url.prefix == "" and is_not_blob and not url.prefix.endswith(
                '/'):
            prefix = url.prefix + '/'
        else:
            prefix = url.prefix

        iterator = self._bucket.list_blobs(
            versions=True, prefix=prefix, delimiter=delimiter)

        blob_names = _list_blobs(iterator=iterator)

        return blob_names

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    def long_ls(self, url: str,
                recursive: bool = False) -> List[Tuple[str, Optional[Stat]]]:
        """
        List URLs with their stats given the url.

        :param url: Google cloud storage URL
        :param recursive:
            if True, list directories recursively
            if False, list only direct subdirectory
        :return: List of the urls of the blobs found and their stats
        """
        entries = self.ls(url=url, recursive=recursive)

        tuples = []
        for entry in entries:
            tuples.append((entry, self.stat(url=entry)))

        return tuples

    @icontract.require(lambda src: not contains_wildcard(prefix=str(src)))
    @icontract.require(lambda dst: not contains_wildcard(prefix=str(dst)))
    # pylint: disable=invalid-name
    # pylint: disable=too-many-arguments
    def cp(self,
           src: Union[str, pathlib.Path],
           dst: Union[str, pathlib.Path],
           recursive: bool = False,
           no_clobber: bool = False,
           multithreaded: bool = False,
           preserve_posix: bool = False) -> None:
        """
        Copy objects from source to destination URL.

        :param src: Source URL
        :param dst: Destination URL
        :param recursive:
            (from https://cloud.google.com/storage/docs/gsutil/commands/cp)
            Causes directories, buckets, and bucket subdirectories to be copied
            recursively. If you neglect to use this option for an
            upload/download, gswrap will raise an exception and inform you that
            no URL matched. Same behaviour as gsutil as long as no wildcards
            are used.

            | your-bucket before:
            | "empty"

            | client.cp(src="gs://your-bucket/some-dir/",
            | dst="gs://your-bucket/another-dir/", recursive=False)

            | # google.api_core.exceptions.GoogleAPIError: No URLs matched

            | current some-dir:
            | # gs://your-bucket/some-dir/file1
            | # gs://your-bucket/some-dir/dir1/file11

            | # destination URL without slash
            | client.cp(src="gs://your-bucket/some-dir/",
            | dst="gs://your-bucket/another-dir", recursive=True)

            | # another-dir after:
            | # gs://your-bucket/another-dir/file1
            | # gs://your-bucket/another-dir/dir1/file11

            | # destination URL with slash
            | client.cp(src="gs://your-bucket/some-dir/",
            | dst="gs://your-bucket/another-dir/", recursive=True)

            | # another-dir after:
            | # gs://your-bucket/another-dir/some-dir/file1
            | # gs://your-bucket/another-dir/some-dir/dir1/file11
        :param no_clobber:
            (from https://cloud.google.com/storage/docs/gsutil/commands/cp)
            When specified, existing files or objects at the destination will
            not be overwritten.
        :param multithreaded:
            if set to False the copy will be performed single-threaded.
            If set to True it will use multiple threads to perform the copy.
        :param preserve_posix:
            (from https://cloud.google.com/storage/docs/gsutil/commands/cp)
            Causes POSIX attributes to be preserved when objects are copied.
            With this feature enabled, gsutil cp will copy fields provided by
            stat. These are the user ID of the owner, the group ID of the
            owning group, the mode (permissions) of the file, and the
            access/modification time of the file. POSIX attributes are always
            preserved when blob is copied on google cloud storage.
        """
        src_str = src if isinstance(src, str) else src.as_posix()
        dst_str = dst if isinstance(dst, str) else dst.as_posix()

        src_url = resource_type(res_loc=str(src_str))
        dst_url = resource_type(res_loc=str(dst_str))

        if isinstance(src_url, _GCSURL) and isinstance(dst_url, _GCSURL):
            self._cp(
                src=src_url,
                dst=dst_url,
                recursive=recursive,
                no_clobber=no_clobber,
                multithreaded=multithreaded)
        elif isinstance(src_url, _GCSURL):
            assert isinstance(dst_url, str)
            self._download(
                src=src_url,
                dst=dst_url,
                recursive=recursive,
                no_clobber=no_clobber,
                multithreaded=multithreaded,
                preserve_posix=preserve_posix)
        elif isinstance(dst_url, _GCSURL):
            assert isinstance(src_url, str)
            self._upload(
                src=src_url,
                dst=dst_url,
                recursive=recursive,
                no_clobber=no_clobber,
                multithreaded=multithreaded,
                preserve_posix=preserve_posix)
        else:
            assert isinstance(src_url, str)
            assert isinstance(dst_url, str)
            # both local
            if no_clobber and pathlib.Path(dst_str).exists():
                # exists, do not overwrite
                return

            src_path = pathlib.Path(src_str)
            if not src_path.exists():
                raise ValueError("Source doesn't exist. Cannot copy {} to {}"
                                 "".format(src_str, dst_str))

            if src_path.is_file():
                shutil.copy(src_str, dst_str)
            elif src_path.is_dir():
                if not recursive:
                    raise ValueError("Source is dir. Cannot copy {} to {} "
                                     "(Did you mean to do "
                                     "cp recursive?)".format(src_str, dst_str))

                shutil.copytree(src_str, dst_str)

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    def _cp(self,
            src: _GCSURL,
            dst: _GCSURL,
            recursive: bool = False,
            no_clobber: bool = False,
            multithreaded: bool = False) -> None:
        """
        Copy blobs from source to destination google cloud URL.

        :param src: source google cloud URL
        :param dst: destination google cloud URL
        :param recursive: if true also copy files within folders
        :param no_clobber: if true don't overwrite files which already exist
        :param multithreaded:
            if set to False the copy will be performed single-threaded.
            If set to True it will use multiple threads to perform the copy.
        """
        # None is ThreadPoolExecutor max_workers default. 1 is single-threaded
        max_workers = None if multithreaded else 1

        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) \
                as executor:

            if recursive:
                delimiter = ''
            else:
                delimiter = '/'

            if src.prefix.endswith('/'):
                src_prefix = src.prefix[:-1]
            else:
                src_prefix = src.prefix

            self._change_bucket(bucket_name=src.bucket)
            src_bucket = self._bucket

            first_page = src_bucket.list_blobs(
                prefix=src_prefix, delimiter=delimiter)._next_page()
            num_items = first_page.num_items
            if num_items == 0:
                raise google.api_core.exceptions.GoogleAPIError(
                    'No URLs matched')

            src_is_dir = num_items > 1
            if not recursive and src_is_dir:
                raise ValueError(
                    "Cannot copy gs://{}/{} to gs://{}/{} (Did you mean to do "
                    "cp recursive?)".format(src.bucket, src.prefix, dst.bucket,
                                            dst.prefix))

            dst_bucket = self._client.get_bucket(bucket_name=dst.bucket)
            blobs_iterator = src_bucket.list_blobs(
                prefix=src_prefix, delimiter=delimiter)
            for blob in blobs_iterator:
                blob_name = _rename_destination_blob(
                    blob_name=blob.name, src=src, dst=dst).as_posix()

                # skip already existing blobs to not overwrite them
                if no_clobber and dst_bucket.get_blob(
                        blob_name=blob_name) is not None:
                    continue

                copy_thread = executor.submit(
                    src_bucket.copy_blob,
                    blob=blob,
                    destination_bucket=dst_bucket,
                    new_name=blob_name)
                futures.append(copy_thread)

        for future in futures:
            future.result()

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-branches
    def _upload(self,
                src: str,
                dst: _GCSURL,
                recursive: bool = False,
                no_clobber: bool = False,
                multithreaded: bool = False,
                preserve_posix: bool = False) -> None:
        """
        Upload objects from local source to google cloud destination.

        :param src: local source path
        :param dst: destination google cloud URL
        :param recursive: if true also upload files within folders
        :param no_clobber: if true don't overwrite files which already exist
        :param multithreaded:
            if set to False the upload will be performed single-threaded.
            If set to True it will use multiple threads to perform the upload.
        """
        upload_files = []  # type: List[str]

        # copy one file to one location incl. renaming
        src_path = pathlib.Path(src)
        src_is_file = src_path.is_file()
        if src_is_file:
            upload_files.append(src_path.name)
            src = src_path.parent.as_posix()
        elif recursive:
            # pylint: disable=unused-variable
            for root, dirs, files in os.walk(src):
                for file_name in files:
                    path = os.path.join(root, file_name)
                    prefix = str(path).replace(src, '', 1)
                    if prefix.startswith('/'):
                        prefix = prefix[1:]
                    upload_files.append(prefix)
        else:
            raise ValueError(
                "Cannot upload {} to gs://{}/{} (Did you mean to do cp "
                "recursive?)".format(src, dst.bucket, dst.prefix))

        self._change_bucket(bucket_name=dst.bucket)
        bucket = self._bucket

        # None is ThreadPoolExecutor max_workers default. 1 is single-threaded
        max_workers = None if multithreaded else 1
        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) \
                as executor:
            for file_name in upload_files:
                # pathlib can't join paths when second part starts with '/'
                if file_name.startswith('/'):
                    file_name = file_name[1:]

                dst_is_file = not dst.prefix.endswith('/')
                if src_is_file and dst_is_file:
                    blob_name = pathlib.Path(dst.prefix)
                else:
                    blob_name = pathlib.Path(dst.prefix)
                    if not dst_is_file and not src_is_file:

                        parent_path = pathlib.Path(src).parent
                        src_parent = src.replace(parent_path.as_posix(), "", 1)
                        if src_parent.startswith('/'):
                            src_parent = src_parent[1:]

                        blob_name = blob_name / src_parent / file_name
                    else:
                        blob_name = blob_name / file_name

                # skip already existing blobs to not overwrite them
                blob_name_str = blob_name.as_posix()
                if no_clobber and bucket.get_blob(
                        blob_name=blob_name_str) is not None:
                    continue

                blob = bucket.blob(blob_name=blob_name_str)
                file_path = pathlib.Path(src) / file_name
                upload_thread = executor.submit(
                    _upload_from_path,
                    blob=blob,
                    path=file_path.as_posix(),
                    preserve_posix=preserve_posix)
                futures.append(upload_thread)

        for future in futures:
            future.result()

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-branches
    def _download(self,
                  src: _GCSURL,
                  dst: str,
                  recursive: bool = False,
                  no_clobber: bool = False,
                  multithreaded: bool = False,
                  preserve_posix: bool = False) -> None:
        """
        Download objects from google cloud source to local destination.

        :param src: google cloud source URL
        :param dst: local destination path
        :param recursive: if True also download files within folders
        :param no_clobber: if True don't overwrite files which already exist
        :param multithreaded:
            if set to False the download will be performed single-threaded.
            If set to True it will use multiple threads to perform the download.
        """
        src_prefix_parent = pathlib.Path(src.prefix)
        src_prefix_parent = src_prefix_parent.parent
        src_gcs_prefix_parent = src_prefix_parent.as_posix()

        if recursive:
            delimiter = ''
        else:
            delimiter = '/'

        self._change_bucket(bucket_name=src.bucket)
        bucket = self._bucket

        # remove trailing slash for gsutil-like ls
        src_prefix = src.prefix
        if src.prefix.endswith('/'):
            src_prefix = src_prefix[:-1]

        first_page = bucket.list_blobs(
            prefix=src_prefix, delimiter=delimiter)._next_page()
        num_items = first_page.num_items
        if num_items == 0:
            raise google.api_core.exceptions.GoogleAPIError('No URLs matched')

        dst_path = pathlib.Path(dst)
        blob_iterator = bucket.list_blobs(
            prefix=src_prefix, delimiter=delimiter)

        # None is ThreadPoolExecutor max_workers default. 1 is single-threaded
        max_workers = None if multithreaded else 1
        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) \
                as executor:
            for blob in blob_iterator:
                blob_prefix = blob.name
                file_name = blob_prefix.replace(src_gcs_prefix_parent, '', 1)
                if file_name.startswith('/'):
                    file_name = file_name[1:]

                # check if file_name has no subdirectory
                if file_name.find('/') == -1 and not dst_path.is_dir():
                    file_path = dst_path
                else:
                    file_path = dst_path / file_name

                # skip already existing file to not overwrite it
                if no_clobber and file_path.exists():
                    continue

                download_thread = executor.submit(
                    _download_to_path,
                    blob=blob,
                    path=file_path.as_posix(),
                    preserve_posix=preserve_posix)
                futures.append(download_thread)

        for future in futures:
            future.result()

    def cp_many_to_many(
            self,
            srcs_dsts: Sequence[Tuple[Union[str, pathlib.
                                            Path], Union[str, pathlib.Path]]],
            recursive: bool = False,
            no_clobber: bool = False,
            multithreaded: bool = False,
            preserve_posix: bool = False):
        """
        Perform multiple copy operations in a single function call.

        Each source will be copied to the corresponding destination.
        Only one function call minimizes the overhead and the operations
        can be performed significantly faster.

        :param srcs_dsts: source URLs/paths and destination URLs/paths
        :param recursive:
            (from https://cloud.google.com/storage/docs/gsutil/commands/cp)
            Causes directories, buckets, and bucket subdirectories to be copied
            recursively. If you neglect to use this option for an
            upload/download, gswrap will raise an exception and inform you that
            no URL matched. Same behaviour as gsutil as long as no wildcards
            are used.
        :param no_clobber:
            (from https://cloud.google.com/storage/docs/gsutil/commands/cp)
            When specified, existing files or objects at the destination will
            not be overwritten.
        :param multithreaded:
            if set to False the copy will be performed single-threaded.
            If set to True it will use multiple threads to perform the copy.
        :param preserve_posix:
            (from https://cloud.google.com/storage/docs/gsutil/commands/cp)
            Causes POSIX attributes to be preserved when objects are copied.
            With this feature enabled, gsutil cp will copy fields provided by
            stat. These are the user ID of the owner, the group ID of the
            owning group, the mode (permissions) of the file, and the
            access/modification time of the file. POSIX attributes are always
            preserved when blob is copied on google cloud storage.
        """
        # None is ThreadPoolExecutor max_workers default. 1 is single-threaded
        max_workers = None if multithreaded else 1
        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers) as executor:
            for src, dst in srcs_dsts:
                cp_thread = executor.submit(
                    self.cp,
                    src=src,
                    dst=dst,
                    recursive=recursive,
                    no_clobber=no_clobber,
                    multithreaded=multithreaded,
                    preserve_posix=preserve_posix)
                futures.append(cp_thread)

        for future in futures:
            future.result()

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    # pylint: disable=invalid-name
    # pylint: disable=too-many-locals
    def rm(self, url: str, recursive: bool = False,
           multithreaded: bool = False) -> None:
        """
        Remove blobs at given URL from google cloud storage.

        :param url: google cloud storage URL
        :param recursive: if True remove files within folders
        :param multithreaded:
            if set to False the remove will be performed single-threaded.
            If set to True it will use multiple threads to perform the remove.
        """
        rm_url = resource_type(res_loc=url)
        assert isinstance(rm_url, _GCSURL)

        self._change_bucket(bucket_name=rm_url.bucket)
        bucket = self._bucket

        blob = bucket.get_blob(blob_name=rm_url.prefix)
        if blob is not None:
            bucket.delete_blob(blob_name=blob.name)
        elif not recursive:
            raise ValueError("No URL matched. Cannot remove gs://{}/{} "
                             "(Did you mean to do rm recursive?)".format(
                                 rm_url.bucket, rm_url.prefix))
        else:
            if recursive:
                delimiter = ''
            else:
                delimiter = '/'

            # add trailing slash to achieve gsutil-like ls behaviour
            gcs_url_prefix = rm_url.prefix
            if not gcs_url_prefix.endswith('/'):
                gcs_url_prefix = gcs_url_prefix + '/'

            first_page = bucket.list_blobs(
                prefix=gcs_url_prefix, delimiter=delimiter)._next_page()
            if first_page.num_items == 0:
                raise google.api_core.exceptions.NotFound('No URLs matched')

            blob_iterator = bucket.list_blobs(
                prefix=gcs_url_prefix, delimiter=delimiter)

            # None is ThreadPoolExecutor max_workers default.
            # 1 is single-threaded
            max_workers = None if multithreaded else 1
            futures = []  # type: List[concurrent.futures.Future]
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers) as executor:
                for blob_to_delete in blob_iterator:
                    delete_thread = executor.submit(
                        bucket.delete_blob, blob_name=blob_to_delete.name)
                    futures.append(delete_thread)

            for future in futures:
                future.result()

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    def read_bytes(self, url: str) -> bytes:
        """
        Retrieve the bytes of the blob at the URL.

        The caller is expected to make sure that the file fits in memory.

        :param url: to the blob on the storage
        :return: bytes of the blob
        """
        read_url = resource_type(res_loc=url)
        assert isinstance(read_url, _GCSURL)

        self._change_bucket(bucket_name=read_url.bucket)

        blob = self._bucket.get_blob(blob_name=read_url.prefix)
        if blob is None:
            raise google.api_core.exceptions.NotFound('No URLs matched')

        return blob.download_as_string()

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    def read_text(self, url: str, encoding: str = 'utf-8') -> str:
        """
        Retrieve the text of the blob at the URL.

        The caller is expected to make sure that the file fits in memory.

        :param url: to the blob on the storage
        :param encoding: used to decode the text, defaults to 'utf-8'
        :return: text of the blob
        """
        return self.read_bytes(url=url).decode(encoding=encoding)

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    def write_bytes(self, url: str, data: bytes):
        """
        Write bytes to the storage at the given URL.

        :param url: where to write in the storage
        :param data: what to write
        :return:
        """
        upload_url = resource_type(res_loc=url)
        assert isinstance(upload_url, _GCSURL)

        self._change_bucket(bucket_name=upload_url.bucket)

        blob = self._bucket.blob(blob_name=upload_url.prefix)

        blob.upload_from_string(data=data)

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    def write_text(self, url: str, text: str, encoding: str = 'utf-8'):
        """
        Write bytes to the storage at the given URL.

        :param url: where to write in the storage
        :param text: what to write
        :param encoding: how to encode, defaults to 'utf-8'
        :return:
        """
        self.write_bytes(url=url, data=text.encode(encoding=encoding))

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    def stat(self, url: str) -> Optional[Stat]:
        """
        Retrieve the stat of the object in the Google Cloud Storage.

        :param url: to the object
        :return: object status,
            or None if the object does not exist or is a directory.
        """
        stat_url = resource_type(res_loc=url)
        assert isinstance(stat_url, _GCSURL)

        self._change_bucket(bucket_name=stat_url.bucket)

        blob = self._bucket.get_blob(blob_name=stat_url.prefix)

        if blob is None:
            return None

        result = Stat()

        result.creation_time = blob.time_created
        result.update_time = blob.updated
        result.storage_class = blob.storage_class
        result.content_length = int(blob.size)
        result.crc32c = bytes(blob.crc32c, encoding='utf-8')
        result.md5 = bytes(blob.md5_hash, encoding='utf-8')

        metadata = blob.metadata
        if metadata is not None:
            if 'goog-reserved-file-atime' in metadata:
                result.file_atime = datetime.datetime.utcfromtimestamp(
                    int(metadata['goog-reserved-file-atime']))

            if 'goog-reserved-file-mtime' in metadata:
                result.file_mtime = datetime.datetime.utcfromtimestamp(
                    int(metadata['goog-reserved-file-mtime']))

            if 'goog-reserved-posix-uid' in metadata:
                result.posix_uid = metadata['goog-reserved-posix-uid']

            if 'goog-reserved-posix-gid' in metadata:
                result.posix_gid = metadata['goog-reserved-posix-gid']

            if 'goog-reserved-posix-mode' in metadata:
                result.posix_mode = metadata['goog-reserved-posix-mode']

        return result

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    def same_modtime(self, path: Union[str, pathlib.Path], url: str) -> bool:
        """
        Check if local path and URL have equal modification times (up to secs).

        Mind that you need to copy the object with -P (preserve posix) flag.

        :param path: to the local file
        :param url: URL to an object
        :return: True if the modification time is the same
        """
        timestamp = os.stat(str(path)).st_mtime
        mtime = datetime.datetime.utcfromtimestamp(timestamp).replace(
            microsecond=0)

        url_stat = self.stat(url=url)

        if url_stat is None:
            raise RuntimeError("The URL does not exist: {}".format(url))

        return mtime == url_stat.file_mtime

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    def same_md5(self, path: Union[str, pathlib.Path], url: str) -> bool:
        """
        Check if the MD5 differs between the local file and the blob.

        :param path: to the local file
        :param url:  to the remote object in Google storage
        :return:
            True if the MD5 is the same. False if the checksum differs or
            local file and/or the remote object do not exist.
        """
        url_stat = self.stat(url=url)

        if url_stat is None:
            return False

        pth_str = str(path)
        if not os.path.exists(pth_str):
            return False

        hsh = hashlib.md5()
        # https://googleapis.github.io/google-cloud-python/latest/_modules/google/cloud/storage/_helpers.html
        # default block_size of google cloud storage (_write_buffer_to_hash)
        block_size = 8192
        with open(pth_str, 'rb') as fid:
            while True:
                buf = fid.read(block_size)
                if not buf:
                    break
                hsh.update(buf)

        local_md5 = base64.b64encode(hsh.digest())

        return url_stat.md5 == local_md5

    def md5_hexdigests(self, urls: List[str], multithreaded: bool = False) \
            -> List[Optional[str]]:
        """
        Retrieve hex digests of MD5 checksums for multiple URLs.

        :param urls: URLs to stat and retrieve MD5 of
        :param multithreaded:
            if set to False the retrieving hex digests of md5 checksums  will
            be performed single-threaded.
            If set to True it will use multiple threads to perform the this.
        :return: list of hexdigests;
            if an URL does not exist, the corresponding item is None.
        """
        hexdigests = []  # type: List[Optional[str]]

        # None is ThreadPoolExecutor max_workers default. 1 is single-threaded
        max_workers = None if multithreaded else 1
        stat_futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) \
                as executor:
            for url in urls:
                stat_futures.append(executor.submit(self.stat, url=url))

            for stat_future in stat_futures:
                stat = stat_future.result()
                hexdigests.append(
                    base64.b64decode(stat.md5).
                    hex() if stat is not None else None)

        for future in stat_futures:
            future.result()

        return hexdigests
