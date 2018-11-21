#!/usr/bin/env python
"""Wrap gCloud Storage API for simpler & multi-threaded handling of objects."""

# pylint: disable=protected-access

import concurrent.futures
import os
import pathlib
import re
import shutil
import urllib.parse
from typing import List, Optional, Union  # pylint: disable=unused-import

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


class _Path:
    """
    Store local path.

    :ivar path: local path to a directory or file
    :vartype path: str
    """

    def __init__(self, path: str) -> None:
        """Initialize path.

        :param path: local path
        """
        self.path = path


def classifier(res_loc: str) -> Union[_GCSURL, _Path]:
    """
    Classifies resource into one of the 2 classes.

    :param res_loc: resource location
    :return: class corresponding to the file/directory location
    """
    parsed_path = urllib.parse.urlparse(url=res_loc)

    if parsed_path.scheme == 'gs':
        return _GCSURL(bucket=parsed_path.netloc, prefix=parsed_path.path)

    if parsed_path.scheme == '':
        return _Path(path=parsed_path.path)

    raise google.api_core.exceptions.GoogleAPIError(
        "Unrecognized scheme '{}'.".format(parsed_path.scheme))


_WILDCARDS_RE = re.compile(r'(\*\*|\*|\?|\[[^]]+\])')


def contains_wildcard(prefix: str) -> bool:
    """
    Check if prefix contains any wildcards.

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
    e.g.    Existing blob on Google Cloud
            gs://your-bucket/your-dir/file

            gsutil cp -r gs://your-bucket/your-dir/ gs://your-bucket/copy-dir/
            gs://your-bucket/copy-dir/your-dir/file

            gsutil cp -r gs://your-bucket/your-dir/ gs://your-bucket/copy-dir
            gs://your-bucket/copy-dir/file

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


class Client:
    """Google Cloud Storage Client for simple usage of gsutil commands."""

    def __init__(self,
                 bucket_name: Optional[str] = None,
                 project: Optional[str] = None) -> None:
        """
        Initialize.

        :param bucket_name: name of the active bucket
        :param project:
            the project which the client acts on behalf of. Will
            be passed when creating a topic. If None, falls back to the
            default inferred from the environment.
        """
        if project is not None:
            self._client = google.cloud.storage.Client(project=project)
        else:
            self._client = google.cloud.storage.Client()

        if bucket_name:
            self._bucket = self._client.get_bucket(bucket_name=bucket_name)
        else:
            self._bucket = None

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

        existing blob at your storage:
        gs://your-bucket/something

        ls(url="gs://your-bucket/some", recursive=True)
        will raise a GoogleAPI error because no url is matching

        if there is another blob:
        gs://your-bucket/some/blob

        ls(url="gs://your-bucket/some", recursive=True)
        will return this blob: "gs://your-bucket/some/blob"

        :param url: Google cloud storage URL
        :param recursive: List only direct subdirectories
        :return: List of Google cloud storage URLs according the given URL
        """
        gcs_url = classifier(res_loc=url)  # type: Union[_GCSURL, _Path]

        assert isinstance(gcs_url, _GCSURL)

        blobs = self._ls(url=gcs_url, recursive=recursive)

        blob_list = []
        for element in blobs:
            blob_list.append('gs://' + gcs_url.bucket + '/' + element)

        return blob_list

    def _ls(self, url: _GCSURL, recursive: bool = False) -> List[str]:
        """
        List the files on Google Cloud Storage given the prefix.

        :param url: uniform google cloud url
        :param recursive: List only direct subdirectories
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

    @icontract.require(lambda src: not contains_wildcard(prefix=src))
    @icontract.require(lambda dst: not contains_wildcard(prefix=dst))
    # pylint: disable=invalid-name
    # pylint: disable=too-many-arguments
    def cp(self,
           src: str,
           dst: str,
           recursive: bool = False,
           no_clobber: bool = False,
           max_workers: int = 1) -> None:
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

            client.cp(src="gs://your-bucket/some-dir/",
            dst="gs://your-bucket/another-dir/", recursive=False)
            # google.api_core.exceptions.GoogleAPIError: No URLs matched

            # client.ls(gcs_url=gs://your-bucket/some-dir/, recursive=True)
            # gs://your-bucket/some-dir/file1
            # gs://your-bucket/some-dir/dir1/file11

            # destination URL without slash
            client.cp(src="gs://your-bucket/some-dir/",
            dst="gs://your-bucket/another-dir", recursive=True)
            # client.ls(gcs_url=gs://your-bucket/another-dir/, recursive=True)
            # gs://your-bucket/another-dir/file1
            # gs://your-bucket/another-dir/dir1/file11

            # destination URL with slash
            client.cp(src="gs://your-bucket/some-dir/",
            dst="gs://your-bucket/another-dir/", recursive=True)
            # client.ls(gcs_url=gs://your-bucket/another-dir/, recursive=True)
            # gs://your-bucket/another-dir/some-dir/file1
            # gs://your-bucket/another-dir/some-dir/dir1/file11
        :param no_clobber:
            (from https://cloud.google.com/storage/docs/gsutil/commands/cp)
            When specified, existing files or objects at the destination will
            not be overwritten.
        :param max_workers:
            if max_workers is None, it will default
            to the number of processors on the machine, multiplied by 5,
            assuming that ThreadPoolExecutor is often used to overlap I/O
            instead of CPU work.
        """
        src_url = classifier(res_loc=src)  # type: Union[_GCSURL, _Path]
        dst_url = classifier(res_loc=dst)  # type: Union[_GCSURL, _Path]

        if isinstance(src_url, _GCSURL) and isinstance(dst_url, _GCSURL):
            self._cp(
                src=src_url,
                dst=dst_url,
                recursive=recursive,
                no_clobber=no_clobber,
                max_workers=max_workers)
        elif isinstance(src_url, _GCSURL):
            assert isinstance(dst_url, _Path)
            self._download(
                src=src_url,
                dst=dst_url,
                recursive=recursive,
                no_clobber=no_clobber,
                max_workers=max_workers)
        elif isinstance(dst_url, _GCSURL):
            assert isinstance(src_url, _Path)
            self._upload(
                src=src_url,
                dst=dst_url,
                recursive=recursive,
                no_clobber=no_clobber,
                max_workers=max_workers)
        else:
            assert isinstance(src_url, _Path)
            assert isinstance(dst_url, _Path)
            # both local
            if no_clobber and pathlib.Path(dst).exists():
                # exists, do not overwrite
                return

            src_path = pathlib.Path(src)
            if not src_path.exists():
                raise ValueError("Source doesn't exist. Cannot copy {} to {}"
                                 "".format(src, dst))

            if src_path.is_file():
                shutil.copy(src, dst)
            elif src_path.is_dir():
                if not recursive:
                    raise ValueError("Source is dir. Cannot copy {} to {} "
                                     "(Did you mean to do "
                                     "cp recursive?)".format(src, dst))

                shutil.copytree(src, dst)

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    def _cp(self,
            src: _GCSURL,
            dst: _GCSURL,
            recursive: bool = False,
            no_clobber: bool = False,
            max_workers: int = 1) -> None:
        """
        Copy blobs from source to destination google cloud URL.

        :param src: source google cloud URL
        :param dst: destination google cloud URL
        :param recursive: if true also copy files within folders
        :param no_clobber: if true don't overwrite files which already exist
        :param max_workers:
            if max_workers is None, it will default
            to the number of processors on the machine, multiplied by 5,
            assuming that ThreadPoolExecutor is often used to overlap I/O
            instead of CPU work.
        """
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
                src: _Path,
                dst: _GCSURL,
                recursive: bool = False,
                no_clobber: bool = False,
                max_workers: int = 1) -> None:
        """
        Upload objects from local source to google cloud destination.

        :param src: local source path
        :param dst: destination google cloud URL
        :param recursive: if true also upload files within folders
        :param no_clobber: if true don't overwrite files which already exist
        :param max_workers:
            if max_workers is None, it will default
            to the number of processors on the machine, multiplied by 5,
            assuming that ThreadPoolExecutor is often used to overlap I/O
            instead of CPU work.
        """
        upload_files = []  # type: List[str]

        # copy one file to one location incl. renaming
        src_path = pathlib.Path(src.path)
        src_is_file = src_path.is_file()
        if src_is_file:
            upload_files.append(src_path.name)
            src.path = src_path.parent.as_posix()
        elif recursive:
            # pylint: disable=unused-variable
            for root, dirs, files in os.walk(src.path):
                for file in files:
                    path = os.path.join(root, file)
                    prefix = str(path).replace(src.path, '', 1)
                    if prefix.startswith('/'):
                        prefix = prefix[1:]
                    upload_files.append(prefix)
        else:
            raise ValueError(
                "Cannot upload {} to gs://{}/{} (Did you mean to do cp "
                "recursive?)".format(src.path, dst.bucket, dst.prefix))

        self._change_bucket(bucket_name=dst.bucket)
        bucket = self._bucket
        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) \
                as executor:
            for file in upload_files:
                # pathlib can't join paths when second part starts with '/'
                if file.startswith('/'):
                    file = file[1:]

                dst_is_file = not dst.prefix.endswith('/')
                if src_is_file and dst_is_file:
                    blob_name = pathlib.Path(dst.prefix)
                else:
                    blob_name = pathlib.Path(dst.prefix)
                    if not dst_is_file and not src_is_file:

                        parent_path = pathlib.Path(src.path).parent
                        src_parent = src.path.replace(parent_path.as_posix(),
                                                      "", 1)
                        if src_parent.startswith('/'):
                            src_parent = src_parent[1:]

                        blob_name = blob_name / src_parent / file
                    else:
                        blob_name = blob_name / file

                # skip already existing blobs to not overwrite them
                new_name = blob_name.as_posix()
                if no_clobber and bucket.get_blob(
                        blob_name=new_name) is not None:
                    continue

                blob = bucket.blob(blob_name=new_name)
                file_name = pathlib.Path(src.path) / file
                upload_thread = executor.submit(
                    blob.upload_from_filename, filename=file_name.as_posix())
                futures.append(upload_thread)

        for future in futures:
            future.result()

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-branches
    def _download(self,
                  src: _GCSURL,
                  dst: _Path,
                  recursive: bool = False,
                  no_clobber: bool = False,
                  max_workers: int = 1) -> None:
        """
        Download objects from google cloud source to local destination.

        :param src: google cloud source URL
        :param dst: local destination path
        :param recursive: if yes also download files within folders
        :param no_clobber: if yes don't overwrite files which already exist
        :param max_workers:
            if max_workers is None, it will default
            to the number of processors on the machine, multiplied by 5,
            assuming that ThreadPoolExecutor is often used to overlap I/O
            instead of CPU work.
        """
        dst_path = pathlib.Path(dst.path)
        if not dst_path.exists():
            dst_path.write_text("create file")

        src_prefix_parent = pathlib.Path(src.prefix)
        src_prefix_parent = src_prefix_parent.parent
        src_gcs_prefix_parent = src_prefix_parent.as_posix()

        parent_dir_set = set()

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

        blob_dir_iterator = bucket.list_blobs(
            prefix=src_prefix, delimiter=delimiter)
        for blob in blob_dir_iterator:
            blob_path = pathlib.Path(blob.name)
            parent = blob_path.parent
            parent_str = parent.as_posix()
            parent_str = parent_str.replace(src_gcs_prefix_parent, dst.path)
            parent_dir_set.add(parent_str)

        parent_dir_list = sorted(parent_dir_set)
        for parent_dir in parent_dir_list:
            needed_dirs = pathlib.Path(parent_dir)
            if not needed_dirs.is_file():
                needed_dirs.mkdir(parents=True, exist_ok=True)

        blob_iterator = bucket.list_blobs(
            prefix=src_prefix, delimiter=delimiter)
        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) \
                as executor:
            for file in blob_iterator:
                file_path = file.name
                file_name = file_path.replace(src_gcs_prefix_parent, '', 1)
                if file_name.startswith('/'):
                    file_name = file_name[1:]

                if dst_path.is_file():
                    filename = dst_path.as_posix()
                else:
                    filename = (dst_path / file_name).as_posix()

                # skip already existing file to not overwrite it
                if no_clobber and pathlib.Path(filename).exists():
                    continue

                download_thread = executor.submit(
                    file.download_to_filename, filename=filename)
                futures.append(download_thread)

        for future in futures:
            future.result()

    @icontract.require(lambda url: url.startswith('gs://'))
    @icontract.require(lambda url: not contains_wildcard(prefix=url))
    # pylint: disable=invalid-name
    # pylint: disable=too-many-locals
    def rm(self, url: str, recursive: bool = False,
           max_workers: int = 1) -> None:
        """
        Remove blobs at given URL from google cloud storage.

        :param url: google cloud storage URL
        :param recursive: if yes remove files within folders
        :param max_workers:
            if max_workers is None, it will default
            to the number of processors on the machine, multiplied by 5,
            assuming that ThreadPoolExecutor is often used to overlap I/O
            instead of CPU work.
        """
        gcs_url = classifier(res_loc=url)

        assert isinstance(gcs_url, _GCSURL)

        self._change_bucket(bucket_name=gcs_url.bucket)
        bucket = self._bucket

        blob = bucket.get_blob(blob_name=gcs_url.prefix)
        if blob is not None:
            bucket.delete_blob(blob_name=blob.name)
        elif not recursive:
            raise ValueError("No URL matched. Cannot remove gs://{}/{} "
                             "(Did you mean to do rm recursive?)".format(
                                 gcs_url.bucket, gcs_url.prefix))
        else:
            if recursive:
                delimiter = ''
            else:
                delimiter = '/'

            # add trailing slash to achieve gsutil-like ls behaviour
            gcs_url_prefix = gcs_url.prefix
            if not gcs_url_prefix.endswith('/'):
                gcs_url_prefix = gcs_url_prefix + '/'

            first_page = bucket.list_blobs(
                prefix=gcs_url_prefix, delimiter=delimiter)._next_page()
            if first_page.num_items == 0:
                raise google.api_core.exceptions.NotFound('No URLs matched')

            blob_iterator = bucket.list_blobs(
                prefix=gcs_url_prefix, delimiter=delimiter)
            futures = []  # type: List[concurrent.futures.Future]
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers) as executor:
                for blob_to_delete in blob_iterator:
                    delete_thread = executor.submit(
                        bucket.delete_blob, blob_name=blob_to_delete.name)
                    futures.append(delete_thread)

            for future in futures:
                future.result()
