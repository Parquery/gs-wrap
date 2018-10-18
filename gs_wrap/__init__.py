#!/usr/bin/env python
"""Python3 wrapper for gsutil commands."""

import concurrent.futures
import google.cloud.storage as storage
import google.cloud.exceptions
import os
import re
import shutil
import distutils.dir_util
from typing import Optional, List

import icontract
import pathlib
import urllib.parse


class GCSPathlib:
    """
    Store path uniformly with information about leading and trailing backslash.

    :ivar path: any path given by the link
    :vartype path: pathlib.Path

    :ivar had_trailing_backslash: True if link had a trailing backslash
    :vartype had_trailing_backslash: bool
    """

    def __init__(self, path: str):
        """Initialize GCSPathlib."""
        self.had_trailing_backslash = path.endswith('/')
        self.path = pathlib.Path(path)

    def convert_to_posix(self, add_trailing_backslash: bool=False, remove_leading_backslash: bool=False, is_cloud_URL: bool=True) -> str:
        """Convert path to posix with expected backslashs."""
        posix = self.path.as_posix()

        # root is presented as '.' but in GCS the prefix can never be the root
        # because it's inside of a bucket
        if (posix == '.' or posix == '/') and is_cloud_URL:
            return ''

        if add_trailing_backslash:
            posix = posix + '/'

        if self.starts_with_backslash() and remove_leading_backslash:
            posix = posix[1:]

        return posix

    def starts_with_backslash(self) -> bool:
        """Check if path starts with backslash."""
        return self.path.is_absolute()

    def name_of_parent_dir(self):
        """Give name of parent directory as GCSPathlib path."""
        gcs_parent = GCSPathlib(path=self.path.parent.as_posix())
        return GCSPathlib(path=self.convert_to_posix().replace(gcs_parent.convert_to_posix(), "", 1))


class UniformPath:
    """
    Structure to unify URLs.

    :ivar is_cloud_URL: True for Google Cloud URL and False for local paths
    :vartype is_cloud_URL: bool

    :ivar bucket: name of the bucket
    :vartype bucket: str

    :ivar prefix: name of the prefix
    :vartype prefix: GCSPathlib
    """

    def __init__(self, link: str):
        """Initialize uniform path structure."""
        parsed_link = urllib.parse.urlparse(link)
        if parsed_link.scheme == 'gs':
            self.is_cloud_URL = True
            self.bucket = parsed_link.netloc
            self.prefix = GCSPathlib(path=parsed_link.path)
        else:
            self.is_cloud_URL = False
            self.bucket = None
            self.prefix = GCSPathlib(parsed_link.path)


class GoogleCloudStorageClient:
    """Client for simple usage of gsutil commands."""

    def __init__(self, bucket_name: str, project: str = None) -> None:
        """
        Initialize.

        :param bucket_name: name of the active bucket
        :param credentials: (Optional) The OAuth2 Credentials to use for this client. If not passed (and if no _http object is passed), falls back to the default inferred from the environment.
        :param project: the project which the client acts on behalf of. Will be passed when creating a topic. If not passed, falls back to the default inferred from the environment.
        """
        self._client = storage.Client(project=project)
        self._bucket = self._client.get_bucket(bucket_name=bucket_name)

    def get_client(self) -> storage.Client:
        """Return active client."""
        return self._client

    def get_bucket(self) -> storage.Bucket:
        """Return active bucket."""
        return self._bucket

    def change_bucket(self, bucket_name: str) -> storage.Bucket:
        """
        Change active bucket.

        :param bucket_name: Name of the to activate bucket
        """
        if bucket_name != self._bucket.name:
            self._bucket = self._client.get_bucket(bucket_name=bucket_name)
        return self._bucket

    @staticmethod
    def _contains_wildcard(prefix: str) -> bool:
        """
        Check if prefix contains any wildcards.

        :param prefix: path to a file or a directory
        :return:
        """
        _WILDCARDS_RE = re.compile(r'(\*\*|\*|\?|\[[^]]+\])')

        match_object = _WILDCARDS_RE.search(prefix)
        if match_object is not None:
            return True
        else:
            return False

    @staticmethod
    def list_found_blobs(iterator) -> List[str]:
        """
        Arrange list of blobs in expected gsutil structure.

        :param iterator: google.cloud.iterator.HTTPIterator returned from google.cloud.storage.bucket.Bucket.list_blobs
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

    @icontract.pre(lambda self, link: self._contains_wildcard(prefix=link) is False)
    def ls(self, link: str, dont_recurse: bool = True) -> List[str]:
        """
        List the files on Google Cloud Storage given the prefix.

        :param link: Google cloud storage URL
        :param dont_recurse: List only direct subdirectories
        :return: List of Google cloud storage URLs according the given URL
        """
        url = UniformPath(link=link)

        if not url.is_cloud_URL:
            raise ValueError("{} is not a google cloud storage URL".format(link))

        blobs = self._ls(url=url, dont_recurse=dont_recurse)

        blob_list = []
        for index, element in enumerate(blobs):
            blob_list.append('gs://' + url.bucket + '/' + element)

        return blob_list

    def _ls(self, url: UniformPath, dont_recurse: bool = True) -> List[str]:
        """
        List the files on Google Cloud Storage given the prefix.

        :param prefix: virtual path relative to bucket
        :param bucket_name: The bucket which is used for the ls command. If not passed, active bucket will be used.
        :param dont_recurse: List only direct subdirectories
        :return: List of prefixes according to the given prefix
        """
        tmp_bucket = self.change_bucket(bucket_name=url.bucket)

        if dont_recurse:
            delimiter = '/'
        else:
            delimiter = ''

        # add trailing backslash to limit search to this file/folder
        # Google Cloud Storage prefix have no leading backslash
        prefix = url.prefix.convert_to_posix(add_trailing_backslash=True, remove_leading_backslash=url.is_cloud_URL)
        iterator = tmp_bucket.list_blobs(versions=True, prefix=prefix, delimiter=delimiter)

        blob_names = self.list_found_blobs(iterator=iterator)

        return blob_names

    @icontract.pre(lambda self, src: self._contains_wildcard(prefix=src) is False)
    @icontract.pre(lambda self, dest: self._contains_wildcard(prefix=dest) is False)
    def cp(self, src: str, dest: str) -> None:
        """
        Copy objects from source to destination URL.

        :param src: Source URL
        :param dest: Destination URL
        """

        src_url = UniformPath(link=src)
        dest_url = UniformPath(link=dest)

        if src_url.is_cloud_URL and dest_url.is_cloud_URL:
            self._cp(src=src_url, dest=dest_url)
        elif src_url.is_cloud_URL:
            self._download(src=src_url, dest=dest_url)
        elif dest_url.is_cloud_URL:
            self._upload(src=src_url, dest=dest_url)
        else:
            if src_url.prefix.path.is_file():
                shutil.copy(src, dest)
            elif src_url.prefix.path.is_dir():
                parent = src_url.prefix.name_of_parent_dir()
                shutil.copytree(src, (dest_url.prefix.path / parent.convert_to_posix(remove_leading_backslash=True)).as_posix())
            else:
                raise ValueError("Cannot copy dir into file")

    @staticmethod
    def _cp_blob_name_restructure(blob_name: str, src: UniformPath, dest: UniformPath) -> pathlib.Path:
        """
        Restructure destination blob name to achieve gsutil like cp -r behaviour.

        :param blob_name: name of the blob which is copied
        :param src: source prefix from where the blob is copied
        :param dest: destination prefix to where the blob should be copied
        :return: new destination blob name
        """
        src_suffix = GCSPathlib(path=blob_name[len(src.prefix.convert_to_posix()):])
        src_suffix_str = src_suffix.convert_to_posix(remove_leading_backslash=True)

        if dest.prefix.had_trailing_backslash:

            src_prefix_parent = src.prefix.name_of_parent_dir()
            return dest.prefix.path / src_prefix_parent.convert_to_posix(remove_leading_backslash=True) / src_suffix_str
        else:
            return dest.prefix.path / src_suffix_str

    def _cp(self, src: UniformPath, dest: UniformPath) -> None:
        """
        Copy blobs from source to destination google cloud URL.
        :param src: source google cloud URL
        :param dest: destination google cloud URL
        """
        src_bucket = self.change_bucket(bucket_name=src.bucket)
        dest_bucket = self._client.get_bucket(bucket_name=dest.bucket)
        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            src_prefix = src.prefix.convert_to_posix(remove_leading_backslash=True)

            for blob in src_bucket.list_blobs(prefix=src_prefix):
                new_name = GCSPathlib(path=self._cp_blob_name_restructure(blob_name=blob.name, src=src, dest=dest).as_posix())
                copy_thread = executor.submit(
                    src_bucket.copy_blob,
                    blob=blob,
                    destination_bucket=dest_bucket,
                    new_name=new_name.convert_to_posix(remove_leading_backslash=True))
                futures.append(copy_thread)

    def _upload(self, src: UniformPath, dest: UniformPath) -> None:
        """
        Upload objects from local source to google cloud destination.

        :param src: local source path
        :param dest: destination google cloud URL
        """
        upload_files = []  # type: List[str]

        if src.prefix.path.is_file():
            upload_files.append(src.prefix.path.name)
            src.prefix.path = src.prefix.path.parent
        else:
            for root, dirs, files in os.walk(src.prefix.convert_to_posix(is_cloud_URL=src.is_cloud_URL)):
                if files:
                    for file in files:
                        path = os.path.join(root, file)
                        prefix = GCSPathlib(path=str(path).replace(src.prefix.convert_to_posix(is_cloud_URL=src.is_cloud_URL), ''))
                        upload_files.append(prefix.convert_to_posix(remove_leading_backslash=True, is_cloud_URL=src.is_cloud_URL))

        bucket = self.change_bucket(bucket_name=dest.bucket)
        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for file in upload_files:
                # pathlib can't join paths when second part starts with '/'
                file = GCSPathlib(path=file).convert_to_posix(remove_leading_backslash=True)

                if len(upload_files) == 1 and not dest.prefix.had_trailing_backslash:
                    blob_name = dest.prefix.path
                else:
                    blob_name = dest.prefix.path
                    if dest.prefix.had_trailing_backslash and not len(upload_files) == 1:

                        parent = src.prefix.name_of_parent_dir()
                        # remove / for path concatenation
                        parent = parent.convert_to_posix(remove_leading_backslash=True)
                        blob_name = blob_name / parent / file
                    else:
                        blob_name = blob_name / file

                blob_name = GCSPathlib(path=blob_name.as_posix())
                blob = bucket.blob(blob_name=blob_name.convert_to_posix(remove_leading_backslash=True))

                file_name = src.prefix.path / file
                upload_thread = executor.submit(blob.upload_from_filename, filename=file_name.as_posix())
                futures.append(upload_thread)

    def _download(self, src: UniformPath, dest: UniformPath) -> None:
        """
        Download objects from google cloud source to local destination.

        :param src: google cloud source URL
        :param dest: local destination path
        """
        bucket = self.change_bucket(bucket_name=src.bucket)

        src_prefix_parent = src.prefix.path
        src_prefix_parent = src_prefix_parent.parent
        src_gcs_prefix_parent = GCSPathlib(path=src_prefix_parent.as_posix())
        parent_dir_set = set()
        for blob in bucket.list_blobs(prefix=src.prefix.convert_to_posix(remove_leading_backslash=True)):
            blob_path = GCSPathlib(path=blob.name)
            parent = blob_path.path.parent
            parent_str = parent.as_posix()
            parent_str = parent_str.replace(
                src_gcs_prefix_parent.convert_to_posix(remove_leading_backslash=True), dest.prefix.convert_to_posix(is_cloud_URL=dest.is_cloud_URL))
            parent_dir_set.add(parent_str)

        parent_dir_set = sorted(parent_dir_set)
        for parent in parent_dir_set:
            needed_dirs = pathlib.Path(parent)
            if not needed_dirs.is_file():
                needed_dirs.mkdir(parents=True, exist_ok=True)

        futures = []  # type: List[concurrent.futures.Future]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for file in bucket.list_blobs(prefix=src.prefix.convert_to_posix(remove_leading_backslash=True)):
                file_path = file.name
                file_name = GCSPathlib(path=file_path.replace(src_gcs_prefix_parent.convert_to_posix(remove_leading_backslash=True), ''))
                # pathlib can't join paths when the second part starts with '/'
                file_name = file_name.convert_to_posix(remove_leading_backslash=True)
                print("filename:", (dest.prefix.path / file_name).as_posix())
                print("file_name:", file_name)
                download_thread = executor.submit(
                    file.download_to_filename, filename=(dest.prefix.path / file_name).as_posix())
                futures.append(download_thread)

    def mb(self, bucket_name: str, project: Optional[str] = None) -> None:
        """
        Create a new bucket.

        :param bucket_name: The bucket name to create.
        :param project: (Optional) the project under which the bucket is to be created. If not passed, uses the project set on the client.
        """
        bucket = self._client.create_bucket(bucket_name=bucket_name, project=project)
        assert isinstance(bucket, storage.Bucket)

    @icontract.pre(lambda self, link: self._contains_wildcard(prefix=link) is False)
    def rm(self, link: str) -> None:
        """
        Remove blobs at given URL from google cloud storage.

        :param link: google cloud storage URL
        """

        url = UniformPath(link=link)

        if not url.is_cloud_URL:
            raise ValueError("{} is not a google cloud storage url")

        bucket = self.change_bucket(bucket_name=url.bucket)

        try:
            futures = []  # type: List[concurrent.futures.Future]
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for blob in bucket.list_blobs(prefix=url.prefix.convert_to_posix(add_trailing_backslash=url.prefix.had_trailing_backslash, remove_leading_backslash=True)):
                    delete_thread = executor.submit(bucket.delete_blob(blob_name=blob.name))
                    futures.append(delete_thread)
        except google.cloud.exceptions.NotFound:
            pass
