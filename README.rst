gswrap
======

gs-wrap wraps Google Cloud Storage API for simpler and multi-threaded handling
of objects.

Usage
=====

* Connect to your Google Cloud Storage bucket

The **project** which the client acts on behalf of. Will be passed when
creating a topic. If not passed, falls back to the default inferred from the
environment.

The **bucket** which the client acts on behalf of. Can be either passed when
creating the client or will be retrieved from the first command accessing the
project. Automatically changes the bucket when trying to access another bucket
in the same project.

.. code-block:: python

    import gswrap

    client = gswrap.Client()
    # or
    client = gswrap.Client(project="project-name", bucket_name="my-bucket")

* List objects in your bucket

.. warning::

    Wildcards (\*, \*\*, \?, \[chars\], \[char range\]) are not supported by
    google cloud storage API and neither by gswrap at the moment [2018-11-07].
    Reasons are that the gsutil with wildcards can hardly be equivalently
    reconstructed and that the toplevel search is extremely inefficient.
    More information about gsutil wildcards can be found here:
    `<https://cloud.google.com/storage/docs/gsutil/addlhelp/WildcardNames>`_

.. code-block:: python

    client.ls(gcs_url="gs://your-bucket/your-dir", recursive=False)
    # gs://your-bucket/your-dir/your-subdir1/
    # gs://your-bucket/your-dir/your-subdir2/
    # gs://your-bucket/your-dir/file1

    client.ls(gcs_url="gs://your-bucket/your-dir", recursive=True)
    # gs://your-bucket/your-dir/your-subdir1/file1
    # gs://your-bucket/your-dir/your-subdir2/file1
    # gs://your-bucket/your-dir/file1

* Copy objects within Google Cloud Storage

If both the source and destination URL are cloud URLs from the same provider,
gsutil copies data "in the cloud" (i.e., without downloading to and uploading
from the machine where you run gswrap).

.. note::
    client.cp() runs by default non-multi-threaded. When multi-threading is
    activated, the maximum number of workers is the number of processors on the
    machine, multiplied by 5.

* Copy file within Google Cloud Storage

.. code-block:: python

    # client.ls("gs://your-bucket/", recursive=True):
    # gs://your-bucket/file1
    client.cp(src="gs://your-bucket/file1",
              dst="gs://your-bucket/your-dir/",
              recursive=True)
    # client.ls("gs://your-bucket/", recursive=True):
    # gs://your-bucket/file1
    # gs://your-bucket/your-dir/file1

    # client.ls("gs://your-backup-bucket/", recursive=True):
    # "empty"
    client.cp(src="gs://your-bucket/file1",
              dst="gs://your-backup-bucket/backup-file1",
              recursive=False)
    # client.ls("gs://your-backup-bucket/"):
    # gs://your-backup-bucket/backup-file1

* Copy directory within Google Cloud Storage

.. code-block:: python

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

    # choice to copy multi-threaded. (default=False)
    # When True, the maximum number of threads is equal the number of
    # processors times 5.
    client.cp(src="gs://your-bucket/some-dir/",
    dst="gs://your-bucket/another-dir", recursive=True, multithreaded=True)
    # client.ls(gcs_url=gs://your-bucket/another-dir/, recursive=True)
    # gs://your-bucket/another-dir/file1
    # gs://your-bucket/another-dir/dir1/file11

* Upload objects to Google Cloud Storage

.. note::

    **recursive** causes directories, buckets, and bucket subdirectories to be
    copied recursively. If you neglect to use this option for an upload, gswrap
    will raise an exception and inform you that no URL matched.
    Same behaviour as gsutil as long as no wildcards are used.

.. code-block:: python

    # local directory:
    # /home/user/storage/file1
    # /home/user/storage/file2

    client.cp(src="/home/user/storage/",
              dst="gs://your-bucket/local/",
              recursive=True)
    # client.ls("gs://your-bucket/", recursive=True):
    # gs://your-bucket/local/storage/file1
    # gs://your-bucket/local/storage/file2

* Download objects from Google Cloud Storage

.. note::

    **recursive** causes directories, buckets, and bucket subdirectories to be
    copied recursively. If you neglect to use this option for a download, gswrap
    will raise an exception and inform you that no URL matched.
    Same behaviour as gsutil as long as no wildcards are used.

.. code-block:: python

    import os

    os.stat("/home/user/storage/file1").st_mtime # 1537947563

    client.cp(src="gs://your-bucket/file1", dst="/home/user/storage/file1",
    no_clobber=True)

    # no_clobber option stops from overwriting
    os.stat("/home/user/storage/file1").st_mtime # 1537947563

    client.cp(src="gs://your-bucket/file1", dst="/home/user/storage/file1",
    no_clobber=False)

    os.stat("/home/user/storage/file1").st_mtime # 1540889799

* Perform multiple copy operations in one call

.. code-block:: python

    sources_destinations = [
                    # copy on google cloud storage
                    ('gs://your-bucket/your-dir/file',
                    'gs://your-bucket/backup-dir/file'),
                    # copy from gcs to local
                    ('gs://your-bucket/your-dir/file',
                    pathlib.Path('/home/user/storage/backup-file')),
                    # copy from local to gcs
                    (pathlib.Path('/home/user/storage/new-file'),
                    'gs://your-bucket/your-dir/new-file'),
                    # copy locally
                    (pathlib.Path('/home/user/storage/file'),
                    pathlib.Path('/home/user/storage/new-file'))
                ]
    client.cp_many_to_many(srcs_dsts=sources_destinations)

* Remove files from google cloud storage

.. code-block:: python

    # existing files:
    # gs://your-bucket/file
    client.rm(url="gs://your-bucket/file")
    # bucket is now empty

    # existing files:
    # gs://your-bucket/file1
    # gs://your-bucket/your-dir/file2
    # gs://your-bucket/your-dir/sub-dir/file3
    client.rm(url="gs://your-bucket/your-dir", recursive=True)
    # remaining files:
    # gs://your-bucket/file1

* Read and write files in google cloud storage

.. code-block:: python

    client.write_text(url:"gs://your-bucket/file", text="Hello, I'm text",
                     encoding='utf-8')
    client.read_text(url:"gs://your-bucket/file", encoding='utf-8')
    # Hello I'm text

    client.write_bytes(url="gs://your-bucket/data",
                        data="I'm important data".encode('utf-8'))

    data = client.read_bytes(url="gs://your-bucket/data")
    print(data.decode('utf-8')) # I'm important data

* Copy os.stat() of a file or metadata of a blob

.. note::

    When copying locally [on remote], stats [metadata] are always preserved.
    **preserve_posix** is only needed when downloading and uploading files.

.. code-block:: python

    file = pathlib.Path('/home/user/storage/file')
    file.touch()
    print(file.stat())
    # os.stat_result(st_mode=33204, st_ino=19022665, st_dev=64769, st_nlink=1,
    # st_uid=1000, st_gid=1000, st_size=0, st_atime=1544015997,
    # st_mtime=1544015997, st_ctime=1544015997)

    # upload without preserve_posix
    client.cp(src=pathlib.Path('/home/user/storage/file'),
                dst="gs://your-bucket/file")

    stats = client.stat(url="gs://your-bucket/file")
    stats.creation_time  # 2018-11-21 13:27:46.255000+00:00
    stats.update_time  # 2018-11-21 13:27:46.255000+00:00
    stats.content_length  # 1024 [bytes]
    stats.storage_class  # REGIONAL
    stats.file_atime  # None
    stats.file_mtime  # None
    stats.posix_uid  # None
    stats.posix_gid  # None
    stats.posix_mode  # None
    stats.md5  # b'1B2M2Y8AsgTpgAmY7PhCfg=='
    stats.crc32c  # b'AAAAAA=='

    # upload with preserve_posix also copies POSIX arguments to blob
    # also works for downloading

    client.cp(src=pathlib.Path('/home/user/storage/file'),
                dst="gs://your-bucket/file", preserve_posix=True)

    stats = client.stat(url="gs://your-bucket/file")
    stats.creation_time  # 2018-11-21 13:27:46.255000+00:00
    stats.update_time  # 2018-11-21 13:27:46.255000+00:00
    stats.content_length  # 1024 [bytes]
    stats.storage_class  # REGIONAL
    stats.file_atime  # 2018-11-21 13:27:46
    stats.file_mtime  # 2018-11-21 13:27:46
    stats.posix_uid  # 1000
    stats.posix_gid  # 1000
    stats.posix_mode  # 777
    stats.md5  # b'1B2M2Y8AsgTpgAmY7PhCfg=='
    stats.crc32c  # b'AAAAAA=='

* Check correctness of copied file

.. code-block:: python

    # check modification time when copied with preserve_posix
    client.same_modtime(path='/home/user/storage/file',
                        url='gs://your-bucket/file')

    # check md5 hash to ensure content equality
    client.same_md5(path='/home/user/storage/file', url='gs://your-bucket/file')

    # retrieves hex digests of MD5 checksums for multiple URLs.
    urls = ['gs://your-bucket/file1', 'gs://your-bucket/file2']
    client.md5_hexdigests(urls=urls, multithreaded=False)

Installation
============

* Install gs-wrap with pip:

.. code-block:: bash

    pip3 install gs-wrap


Development
===========

* Check out the repository.

* In the repository root, create the virtual environment:

.. code-block:: bash

    python3 -m venv venv3

* Activate the virtual environment:

.. code-block:: bash

    source venv3/bin/activate

* Install the development dependencies:

.. code-block:: bash

    pip3 install -e .[dev]

We use tox for testing and packaging the distribution. Assuming that the virtual
environment has been activated and the development dependencies have been
installed, run:

.. code-block:: bash

    tox


Pre-commit Checks
-----------------

We provide a set of pre-commit checks that lint and check code for formatting.

Namely, we use:

* `yapf <https://github.com/google/yapf>`_ to check the formatting.
* The style of the docstrings is checked with `pydocstyle <https://github.com/PyCQA/pydocstyle>`_.
* Static type analysis is performed with `mypy <http://mypy-lang.org/>`_.
* `isort <https://github.com/timothycrosley/isort>`_ to sort your imports for you.
* Various linter checks are done with `pylint <https://www.pylint.org/>`_.
* Doctests are executed using the Python `doctest module <https://docs.python.org/3.5/library/doctest.html>`_.

Run the pre-commit checks locally from an activated virtual environment with development dependencies:

.. code-block:: bash

    ./precommit.py

* The pre-commit script can also automatically format the code:

.. code-block:: bash

    ./precommit.py  --overwrite


Versioning
==========
We follow `Semantic Versioning <http://semver.org/spec/v1.0.0.html>`_. The version X.Y.Z indicates:

* X is the major version (backward-incompatible),
* Y is the minor version (backward-compatible), and
* Z is the patch version (backward-compatible bug fix).
