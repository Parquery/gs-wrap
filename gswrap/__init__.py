#!/usr/bin/env python
"""Python3 wrapper for gsutil commands."""

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
# pylint: disable=fixme
# TODO(snaji): remove 'as storage' when finished
import google.cloud.storage as storage  # pylint: disable=useless-import-alias

import icontract


class _GCSPathlib:
    """
    Store path uniformly with information about leading and trailing backslash.

    :ivar path: any path given by the resource location
    :vartype path: pathlib.Path

    :ivar _had_trailing_backslash: True if resource location had a trailing
    backslash
    :vartype had_trailing_backslash: bool
    """

    def __init__(self, path: str) -> None:
        """Initialize GCSPathlib."""
        self._had_trailing_backslash = path.endswith('/')
        self._path = pathlib.Path(path)

    def _convert_to_posix(self,
                          add_trailing_backslash: bool = False,
                          remove_leading_backslash: bool = False,
                          is_cloud_url: bool = True) -> str:
        """Convert path to posix with expected backslashs."""
        posix = self._path.as_posix()

        # cwd (current working directory) is presented as '.' but GCS doesn't
        # know the concept of cwd because there are only urls to blobs
        if posix in ('.', '/') and is_cloud_url:
            return ''

        if add_trailing_backslash:
            posix = posix + '/'

        if self._starts_with_backslash() and remove_leading_backslash:
            posix = posix[1:]

        return posix

    def _starts_with_backslash(self) -> bool:
        """Check if path starts with backslash."""
        return self._path.is_absolute()

    def _name_of_parent_dir(self):
        """Give name of parent directory as GCSPathlib path."""
        gcs_parent = _GCSPathlib(path=self._path.parent.as_posix())
        return _GCSPathlib(path=self._convert_to_posix().replace(
            gcs_parent._convert_to_posix(), "", 1))


class _UniformPath:
    """
    Structure to unify URLs.

    :ivar is_cloud_url: True for Google Cloud URL and False for local paths
    :vartype is_cloud_url: bool

    :ivar bucket: name of the bucket
    :vartype bucket: str

    :ivar prefix: name of the prefix
    :vartype prefix: GCSPathlib
    """

    def __init__(self, res_loc: str) -> None:
        """Initialize uniform path structure.

        :param res_loc: Resource location
        """
        parsed_rec_loc = urllib.parse.urlparse(res_loc)
        if parsed_rec_loc.scheme == 'gs':
            self.is_cloud_url = True
            self.bucket = parsed_rec_loc.netloc
            self.prefix = _GCSPathlib(path=parsed_rec_loc.path)
        else:
            self.is_cloud_url = False
            self.bucket = ""
            self.prefix = _GCSPathlib(parsed_rec_loc.path)


_WILDCARDS_RE = re.compile(r'(\*\*|\*|\?|\[[^]]+\])')


def _contains_wildcard(prefix: str) -> bool:
    """
    Check if prefix contains any wildcards.

    :param prefix: path to a file or a directory
    :return:
    """
    match_object = _WILDCARDS_RE.search(prefix)
    return match_object is not None


def _list_found_blobs(
        iterator: google.api_core.page_iterator.HTTPIterator) -> List[str]:
    """
    Arrange list of blobs in expected gsutil structure.

    :param iterator: google.cloud.iterator.HTTPIterator returned from
    google.cloud.storage.bucket.Bucket.list_blobs
    :return: List of blobs
    """
    subdirectories = iterator.prefixes
    objects = list(iterator)
    subdirectories = sorted(subdirectories)
    blob_names = []  # type: List[str]

    for obj in objects:
        blob_names.append(obj.name)

    for subdir in subdirectories:
        blob_names.append(subdir)

    return blob_names


def _cp_blob_name_restructure(blob_name: str, src: _UniformPath,
                              dst: _UniformPath) -> pathlib.Path:
    """
    Restructure destination blob name to achieve gsutil like cp -r behaviour.

    :param blob_name: name of the blob which is copied
    :param src: source prefix from where the blob is copied
    :param dst: destination prefix to where the blob should be copied
    :return: new destination blob name
    """
    src_suffix = _GCSPathlib(
        path=blob_name[len(src.prefix._convert_to_posix()):])
    src_suffix_str = src_suffix._convert_to_posix(remove_leading_backslash=True)

    if dst.prefix._had_trailing_backslash:
        src_prefix_parent = src.prefix._name_of_parent_dir()
        return dst.prefix._path / src_prefix_parent._convert_to_posix(
            remove_leading_backslash=True) / src_suffix_str

    return dst.prefix._path / src_suffix_str


class Client:
    """Google Cloud Storage Client for simple usage of gsutil commands."""

    # TODO(snaji): maybe add credentials
    def __init__(self, bucket_name: Optional[str] = None,
                 project: str = None) -> None:
        """
        Initialize.

        :param bucket_name: name of the active bucket
        :param project: the project which the client acts on behalf of. Will
        be passed when creating a topic. If not passed, falls back to the
        default inferred from the environment.
        """
        self._client = storage.Client(project=project)
        if bucket_name:
            self._bucket = self._client.get_bucket(bucket_name=bucket_name)
        else:
            self._bucket = None

    def _change_bucket(self, bucket_name: str):
        """
        Change active bucket.

        :param bucket_name: Name of the to activate bucket
        """
        if not self._bucket or bucket_name != self._bucket.name:
            self._bucket = self._client.get_bucket(bucket_name=bucket_name)

    @icontract.require(
        lambda self, gcs_url: not _contains_wildcard(prefix=gcs_url))
    def ls(self, gcs_url: str, recursive: bool = False) -> List[str]:  # pylint: disable=invalid-name
        """
        List the files on Google Cloud Storage given the prefix.

        :param gcs_url: Google cloud storage URL
        :param recursive: List only direct subdirectories
        :return: List of Google cloud storage URLs according the given URL
        """
        url = _UniformPath(res_loc=gcs_url)

        if not url.is_cloud_url:
            raise ValueError(
                "{} is not a google cloud storage URL".format(gcs_url))

        blobs = self._ls(url=url, recursive=recursive)

        blob_list = []
        for element in blobs:
            blob_list.append('gs://' + url.bucket + '/' + element)

        return blob_list

    def _ls(self, url: _UniformPath, recursive: bool = False) -> List[str]:
        """
        List the files on Google Cloud Storage given the prefix.

        :param url: uniform google cloud url
        :param recursive: List only direct subdirectories
        :return: List of prefixes according to the given prefix
        """
        if recursive:
            delimiter = ''
        else:
            delimiter = '/'

        # add trailing backslash to limit search to this file/folder
        # Google Cloud Storage prefix have no leading backslash
        prefix = url.prefix._convert_to_posix(
            add_trailing_backslash=True,
            remove_leading_backslash=url.is_cloud_url)
        self._change_bucket(bucket_name=url.bucket)
        iterator = self._bucket.list_blobs(
            versions=True, prefix=prefix, delimiter=delimiter)

        blob_names = _list_found_blobs(iterator=iterator)

        return blob_names

    # pylint: disable=invalid-name
    @icontract.require(lambda self, src: not _contains_wildcard(prefix=src))
    @icontract.require(lambda self, dst: not _contains_wildcard(prefix=dst))
    def cp(self,
           src: str,
           dst: str,
           recursive: bool = False,
           no_clobber: bool = False) -> None:
        """
        Copy objects from source to destination URL.

        :param src: Source URL
        :param dst: Destination URL
        :param recursive:
        (from https://cloud.google.com/storage/docs/gsutil/commands/cp)
        Causes directories, buckets, and bucket subdirectories to be copied
        recursively. If you neglect to use this option for an upload, gsutil
        will copy any files it finds and skip any directories. Similarly,
        neglecting to specify this option for a download will cause gsutil to
        copy any objects at the current bucket directory level, and skip any
        subdirectories.
        :param no_clobber:
        (from https://cloud.google.com/storage/docs/gsutil/commands/cp)
        When specified, existing files or objects at the destination will not
        be overwritten.
        """
        src_url = _UniformPath(res_loc=src)
        dst_url = _UniformPath(res_loc=dst)

        if src_url.is_cloud_url and dst_url.is_cloud_url:
            self._cp(
                src=src_url,
                dst=dst_url,
                recursive=recursive,
                no_clobber=no_clobber)
        elif src_url.is_cloud_url:
            self._download(
                src=src_url,
                dst=dst_url,
                recursive=recursive,
                no_clobber=no_clobber)
        elif dst_url.is_cloud_url:
            self._upload(
                src=src_url,
                dst=dst_url,
                recursive=recursive,
                no_clobber=no_clobber)
        else:
            if not no_clobber or not dst_url.prefix._path.exists():
                if src_url.prefix._path.is_file():
                    shutil.copy(src, dst)
                elif src_url.prefix._path.is_dir() and recursive:
                    parent = src_url.prefix._name_of_parent_dir()
                    shutil.copytree(
                        src, (dst_url.prefix._path / parent._convert_to_posix(
                            remove_leading_backslash=True)).as_posix())
                else:
                    raise ValueError(
                        "Cannot copy {} to {} (Did you mean to do cp "
                        "recursive?)".format(src, dst))

    def _cp(self,
            src: _UniformPath,
            dst: _UniformPath,
            recursive: bool = False,
            no_clobber: bool = False) -> None:
        """
        Copy blobs from source to destination google cloud URL.

        :param src: source google cloud URL
        :param dst: destination google cloud URL
        :param recursive: if true also copy files within folders
        :param no_clobber: if true don't overwrite files which already exist
        """
        # pylint: disable=too-many-locals
        if recursive:
            delimiter = ''
        else:
            delimiter = '/'

        self._change_bucket(bucket_name=src.bucket)
        src_bucket = self._bucket
        dst_bucket = self._client.get_bucket(bucket_name=dst.bucket)
        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            src_prefix = src.prefix._convert_to_posix(
                remove_leading_backslash=True)

            list_of_blobs = list(
                src_bucket.list_blobs(prefix=src_prefix, delimiter=delimiter))

            if not recursive and len(list_of_blobs) > 1:
                raise ValueError(
                    "Cannot copy gs://{}/{} to gs://{}/{} (Did you mean to do "
                    "cp recursive?)".format(
                        src.bucket,
                        src.prefix._convert_to_posix(
                            add_trailing_backslash=src.prefix.
                            _had_trailing_backslash,
                            remove_leading_backslash=True), dst.bucket,
                        dst.prefix._convert_to_posix(
                            add_trailing_backslash=dst.prefix.
                            _had_trailing_backslash,
                            remove_leading_backslash=True)))

            for blob in list_of_blobs:
                new_name = _GCSPathlib(
                    path=_cp_blob_name_restructure(
                        blob_name=blob.name, src=src, dst=dst).as_posix())

                # skip already existing blobs to not overwrite them
                if no_clobber and dst_bucket.get_blob(
                        blob_name=new_name._convert_to_posix(
                            remove_leading_backslash=True)) is not None:
                    continue

                copy_thread = executor.submit(
                    src_bucket.copy_blob,
                    blob=blob,
                    destination_bucket=dst_bucket,
                    new_name=new_name._convert_to_posix(
                        remove_leading_backslash=True))
                futures.append(copy_thread)

    def _upload(self,
                src: _UniformPath,
                dst: _UniformPath,
                recursive: bool = False,
                no_clobber: bool = False) -> None:
        """
        Upload objects from local source to google cloud destination.

        :param src: local source path
        :param dst: destination google cloud URL
        :param recursive: if true also upload files within folders
        :param no_clobber: if true don't overwrite files which already exist
        """
        # pylint: disable=too-many-locals
        upload_files = []  # type: List[str]

        # copy one file to one location incl. renaming
        if src.prefix._path.is_file():
            upload_files.append(src.prefix._path.name)
            src.prefix._path = src.prefix._path.parent
            src_is_file = True
        elif recursive:
            src_is_file = False
            # pylint: disable=unused-variable
            for root, dirs, files in os.walk(
                    src.prefix._convert_to_posix(
                        is_cloud_url=src.is_cloud_url)):
                if files:
                    for file in files:
                        path = os.path.join(root, file)
                        prefix = _GCSPathlib(
                            path=str(path).replace(
                                src.prefix._convert_to_posix(
                                    is_cloud_url=src.is_cloud_url), ''))
                        upload_files.append(
                            prefix._convert_to_posix(
                                remove_leading_backslash=True,
                                is_cloud_url=src.is_cloud_url))
        else:
            raise ValueError(
                "Cannot upload {} to gs://{}/{} (Did you mean to do cp "
                "recursive?)".format(
                    src.prefix._convert_to_posix(
                        add_trailing_backslash=src.prefix.
                        _had_trailing_backslash,
                        remove_leading_backslash=True), dst.bucket,
                    dst.prefix._convert_to_posix(
                        add_trailing_backslash=dst.prefix.
                        _had_trailing_backslash,
                        remove_leading_backslash=True)))

        self._change_bucket(bucket_name=dst.bucket)
        bucket = self._bucket
        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for file in upload_files:
                # pathlib can't join paths when second part starts with '/'
                file = _GCSPathlib(path=file)._convert_to_posix(
                    remove_leading_backslash=True)

                if src_is_file and not dst.prefix._had_trailing_backslash:
                    blob_name = dst.prefix._path
                else:
                    blob_name = dst.prefix._path
                    if dst.prefix._had_trailing_backslash and not src_is_file:

                        parent = src.prefix._name_of_parent_dir()
                        # remove / for path concatenation
                        parent = parent._convert_to_posix(
                            remove_leading_backslash=True)
                        blob_name = blob_name / parent / file
                    else:
                        blob_name = blob_name / file

                gcs_blob_name = _GCSPathlib(path=blob_name.as_posix())

                # skip already existing blobs to not overwrite them
                if no_clobber and bucket.get_blob(
                        blob_name=gcs_blob_name._convert_to_posix(
                            remove_leading_backslash=True)) is not None:
                    continue

                blob = bucket.blob(
                    blob_name=gcs_blob_name._convert_to_posix(
                        remove_leading_backslash=True))

                file_name = src.prefix._path / file
                upload_thread = executor.submit(
                    blob.upload_from_filename, filename=file_name.as_posix())
                futures.append(upload_thread)

    def _download(self,
                  src: _UniformPath,
                  dst: _UniformPath,
                  recursive: bool = False,
                  no_clobber: bool = False) -> None:
        """
        Download objects from google cloud source to local destination.

        :param src: google cloud source URL
        :param dst: local destination path
        :param recursive: if yes also download files within folders
        :param no_clobber: if yes don't overwrite files which already exist
        """
        # pylint: disable=too-many-locals
        if recursive:
            delimiter = ''
        else:
            delimiter = '/'

        self._change_bucket(bucket_name=src.bucket)
        bucket = self._bucket

        if not dst.prefix._path.exists():
            dst.prefix._path.write_text("create file")

        src_prefix_parent = src.prefix._path
        src_prefix_parent = src_prefix_parent.parent
        src_gcs_prefix_parent = _GCSPathlib(path=src_prefix_parent.as_posix())
        parent_dir_set = set()
        for blob in bucket.list_blobs(
                prefix=src.prefix._convert_to_posix(
                    remove_leading_backslash=True),
                delimiter=delimiter):
            blob_path = _GCSPathlib(path=blob.name)
            parent = blob_path._path.parent
            parent_str = parent.as_posix()
            parent_str = parent_str.replace(
                src_gcs_prefix_parent._convert_to_posix(
                    remove_leading_backslash=True),
                dst.prefix._convert_to_posix(is_cloud_url=dst.is_cloud_url))
            parent_dir_set.add(parent_str)

        parent_dir_list = sorted(parent_dir_set)
        for parent_dir in parent_dir_list:
            needed_dirs = pathlib.Path(parent_dir)
            if not needed_dirs.is_file():
                needed_dirs.mkdir(parents=True, exist_ok=True)

        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for file in bucket.list_blobs(
                    prefix=src.prefix._convert_to_posix(
                        remove_leading_backslash=True),
                    delimiter=delimiter):
                file_path = file.name
                file_name = _GCSPathlib(
                    path=file_path.replace(
                        src_gcs_prefix_parent._convert_to_posix(
                            remove_leading_backslash=True), ''))
                # pathlib can't join paths when the second part starts with '/'
                file_name_str = file_name._convert_to_posix(
                    remove_leading_backslash=True)

                if dst.prefix._path.is_file():
                    filename = dst.prefix._path.as_posix()
                else:
                    filename = (dst.prefix._path / file_name_str).as_posix()

                # skip already existing file to not overwrite it
                if no_clobber and pathlib.Path(filename).exists():
                    continue

                download_thread = executor.submit(
                    file.download_to_filename, filename=filename)
                futures.append(download_thread)

    @icontract.require(
        lambda self, gcs_url: not _contains_wildcard(prefix=gcs_url))
    def rm(self, gcs_url: str, recursive: bool = False) -> None:  # pylint: disable=invalid-name
        """
        Remove blobs at given URL from google cloud storage.

        :param gcs_url: google cloud storage URL
        :param recursive: if yes remove files within folders
        """
        if recursive:
            delimiter = ''
        else:
            delimiter = '/'

        url = _UniformPath(res_loc=gcs_url)

        if not url.is_cloud_url:
            raise ValueError("{} is not a google cloud storage url")

        self._change_bucket(bucket_name=url.bucket)
        bucket = self._bucket

        try:
            futures = []  # type: List[concurrent.futures.Future]
            with concurrent.futures.ThreadPoolExecutor() as executor:
                blob_prefix = url.prefix._convert_to_posix(
                    add_trailing_backslash=url.prefix._had_trailing_backslash,
                    remove_leading_backslash=True)
                blob = bucket.get_blob(blob_name=blob_prefix)
                if not recursive:
                    if blob is None:
                        raise ValueError(
                            "No URL matched. Cannot remove gs://{}/{} "
                            "(Did you mean to do rm recursive?)".format(
                                url.bucket,
                                url.prefix._convert_to_posix(
                                    add_trailing_backslash=url.prefix.
                                    _had_trailing_backslash,
                                    remove_leading_backslash=True)))
                    else:
                        list_of_blobs = [blob]
                else:
                    list_of_blobs = list(
                        bucket.list_blobs(
                            prefix=url.prefix._convert_to_posix(
                                add_trailing_backslash=True,
                                remove_leading_backslash=True),
                            delimiter=delimiter))
                    if blob is not None:
                        list_of_blobs.append(blob)

                for blob in list_of_blobs:
                    delete_thread = executor.submit(
                        bucket.delete_blob(blob_name=blob.name))
                    futures.append(delete_thread)
        except google.api_core.exceptions.NotFound:
            pass
