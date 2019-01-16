gswrap
======

.. image:: https://badges.frapsoft.com/os/mit/mit.png?v=103
    :target: https://opensource.org/licenses/mit-license.php
    :alt: MIT License

.. image:: https://badge.fury.io/py/gs-wrap.svg
    :target: https://badge.fury.io/py/gs-wrap
    :alt: PyPI - version

.. image:: https://img.shields.io/pypi/pyversions/gs-wrap.svg
    :alt: PyPI - Python Version

.. image:: https://readthedocs.org/projects/gs-wrap/badge/?version=latest
    :target: https://gs-wrap.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

``gs-wrap`` wraps Google Cloud Storage API for multi-threaded
data manipulation including
copying, reading, writing and hashing.

Originally, we used our `gsutilwrap <https://github.com/Parquery/gsutilwrap/>`_,
a thin wrapper around ``gsutil`` command-line interface, to simplify
the deployment and backup tasks related to Google Cloud Storage.
However, ``gsutilwrap`` was prohibitely slow at copying many objects from
many different prefix paths to many other prefix paths.

Therefore we developed ``gs-wrap`` to accelerate these operations while keeping
it equally fast or faster than ``gsutilwrap`` at other operations.

While the `google-cloud-storage
<https://github.com/googleapis/google-cloud-python/tree/master/storage/>`_ library
provided by Google offers sophisticated features and good performance, 
its use cases and behavior differ from ``gsutil``. 
Since we wanted the simplicity and usage patterns of ``gsutil``, we made ``gs-wrap``
wrap ``google-cloud-storage`` in its core and set it to behave like ``gsutil``.

Other related projects
----------------------

* `cloud-storage-client <https://github.com/Rakanixu/cloud-storage-client/>`_ to connect with S3 storage and Google Cloud storage

Usage
=====

Connect to your Google Cloud Storage bucket
-------------------------------------------

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
    client = gswrap.Client(
        project="project-name", 
        bucket_name="my-bucket")

List objects in your bucket
---------------------------

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

Copy objects within Google Cloud Storage
----------------------------------------

If both the source and destination URL are cloud URLs from the same provider,
gsutil copies data "in the cloud" (i.e., without downloading to and uploading
from the machine where you run gswrap).

.. note::
    client.cp() runs single-threaded by default. When multi-threading is
    activated, the maximum number of workers is the number of processors on the
    machine, multiplied by 5.

Copy file within Google Cloud Storage
-------------------------------------

.. code-block:: python

    # your-bucket before:
    # gs://your-bucket/file1
    client.cp(src="gs://your-bucket/file1",
              dst="gs://your-bucket/your-dir/",
              recursive=True)
    # your-bucket after:
    # gs://your-bucket/file1
    # gs://your-bucket/your-dir/file1

    # your-backup-bucket before:
    # "empty"
    client.cp(src="gs://your-bucket/file1",
              dst="gs://your-backup-bucket/backup-file1",
              recursive=False)
    # your-backup-bucket after:
    # gs://your-backup-bucket/backup-file1

Copy directory within Google Cloud Storage
------------------------------------------

.. code-block:: python

    # your-bucket before:
    # "empty"
    client.cp(src="gs://your-bucket/some-dir/",
    dst="gs://your-bucket/another-dir/", recursive=False)
    # google.api_core.exceptions.GoogleAPIError: No URLs matched

    # your-bucket before:
    # gs://your-bucket/some-dir/file1
    # gs://your-bucket/some-dir/dir1/file11

    # destination URL without slash
    client.cp(src="gs://your-bucket/some-dir/",
    dst="gs://your-bucket/another-dir", recursive=True)
    # your-bucket after:
    # gs://your-bucket/another-dir/file1
    # gs://your-bucket/another-dir/dir1/file11

    # destination URL with slash
    client.cp(src="gs://your-bucket/some-dir/",
    dst="gs://your-bucket/another-dir/", recursive=True)
    # your-bucket after:
    # gs://your-bucket/another-dir/some-dir/file1
    # gs://your-bucket/another-dir/some-dir/dir1/file11

    # choice to copy multi-threaded. (default=False)
    client.cp(src="gs://your-bucket/some-dir/",
    dst="gs://your-bucket/another-dir", recursive=True, multithreaded=True)
    # your-bucket after:
    # gs://your-bucket/another-dir/file1
    # gs://your-bucket/another-dir/dir1/file11

Upload objects to Google Cloud Storage
--------------------------------------

.. note::

    **recursive** causes directories, buckets, and bucket subdirectories to be
    copied recursively. If you upload from local disk to Google Storage
    and set recursive to ``False``, gswrap
    will raise an exception and inform you that no URL matched.
    This mimicks the behaviour of ``gsutil`` when no wildcards are used.

.. code-block:: python

    # your local directory:
    # /home/user/storage/file1
    # /home/user/storage/file2
    # your-bucket before:
    # "empty"

    client.cp(src="/home/user/storage/",
              dst="gs://your-bucket/local/",
              recursive=True)
    # your-bucket after:
    # gs://your-bucket/local/storage/file1
    # gs://your-bucket/local/storage/file2

Download objects from Google Cloud Storage
------------------------------------------

.. note::

    **recursive** causes directories, buckets, and bucket subdirectories to be
    copied recursively. If you upload from local disk to Google Storage
    and set recursive to ``False``, gswrap
    will raise an exception and inform you that no URL matched.
    This mimicks the behaviour of ``gsutil`` when no wildcards are used.

.. code-block:: python

    import os

    os.stat("/home/user/storage/file1").st_mtime # 1537947563

    client.cp(
        src="gs://your-bucket/file1", 
        dst="/home/user/storage/file1",
        no_clobber=True)

    # no_clobber option stops from overwriting
    os.stat("/home/user/storage/file1").st_mtime # 1537947563

    client.cp(
        src="gs://your-bucket/file1", 
        dst="/home/user/storage/file1",
        no_clobber=False)

    os.stat("/home/user/storage/file1").st_mtime # 1540889799

Perform multiple copy operations in one call
--------------------------------------------

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
         pathlib.Path('/home/user/storage/new-file'))]

    client.cp_many_to_many(srcs_dsts=sources_destinations)

Remove files from google cloud storage
--------------------------------------

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

Read and write files in google cloud storage
--------------------------------------------

.. code-block:: python

    client.write_text(url="gs://your-bucket/file",
                      text="Hello, I'm text",
                      encoding='utf-8')

    client.read_text(url="gs://your-bucket/file", 
                     encoding='utf-8')
    # Hello I'm text

    client.write_bytes(url="gs://your-bucket/data",
                       data="I'm important data".encode('utf-8'))

    data = client.read_bytes(url="gs://your-bucket/data")
    data.decode('utf-8')
    # I'm important data

Copy os.stat() of a file or metadata of a blob
----------------------------------------------

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

Check correctness of copied file
--------------------------------

.. code-block:: python

    # check modification time when copied with preserve_posix
    client.same_modtime(path='/home/user/storage/file',
                        url='gs://your-bucket/file')

    # check md5 hash to ensure content equality
    client.same_md5(path='/home/user/storage/file', url='gs://your-bucket/file')

    # retrieves hex digests of MD5 checksums for multiple URLs.
    urls = ['gs://your-bucket/file1', 'gs://your-bucket/file2']
    client.md5_hexdigests(urls=urls, multithreaded=False)

Documentation
=============
The documentation is available on `readthedocs
<https://gs-wrap.readthedocs.io/en/latest/>`_.

Setup
=====

In order to use this library, you need to go through the following steps:

1. `Select or create a Cloud Platform project. <https://console.cloud.google.com/project>`_
2. `Enable billing for your project. <https://console.cloud.google.com/project>`_
3. `Enable the Google Cloud Storage API. <https://cloud.google.com/storage>`_
4. `Setup Authentication using the Google Cloud SDK. <https://googlecloudplatform.github.io/google-cloud-python/latest/core/auth.html>`_

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
* `pyicontract-lint <https://github.com/Parquery/pyicontract-lint/>`_ lints contracts 
  in Python code defined with `icontract library <https://github.com/Parquery/icontract/>`_.
* `twine <https://pypi.org/project/twine/>`_ to check the README for invalid markup 
  which prevents it from rendering correctly on PyPI.

Run the pre-commit checks locally from an activated virtual environment with
development dependencies:

.. code-block:: bash

    ./precommit.py

* The pre-commit script can also automatically format the code:

.. code-block:: bash

    ./precommit.py  --overwrite

Benchmarks
----------

Assuming that the virtual environment has been activated, the development
dependencies have been installed and the ``PYTHONPATH`` has been set to the
project directory, run the benchmarks with:

.. code-block:: bash

    ./benchmark/main.py *NAME OF YOUR GCS BUCKET*

Here are some of our benchmark results:

.. code-block:: text

    Benchmark list 10000 files:
    +------------+----------------------+----------------------+
    |   Tested   |         Time         |       SpeedUp        |
    +------------+----------------------+----------------------+
    |   gswrap   | 3.5658528804779053 s |          -           |
    | gsutilwrap | 4.134420871734619 s  | 1.1594479666756505 x |
    +------------+----------------------+----------------------+

    Benchmark upload 10000 files:
    +------------+---------------------+----------------------+
    |   Tested   |         Time        |       SpeedUp        |
    +------------+---------------------+----------------------+
    |   gswrap   | 39.73294186592102 s |          -           |
    | gsutilwrap | 70.75882768630981 s | 1.7808605243751086 x |
    +------------+---------------------+----------------------+

    Benchmark upload-many-to-many 500 files:
    +------------+----------------------+---------------------+
    |   Tested   |         Time         |       SpeedUp       |
    +------------+----------------------+---------------------+
    |   gswrap   | 1.8486201763153076 s |          -          |
    | gsutilwrap | 62.999937534332275 s | 34.07943845982712 x |
    +------------+----------------------+---------------------+

    Benchmark download 10000 files:
    +------------+----------------------+----------------------+
    |   Tested   |         Time         |       SpeedUp        |
    +------------+----------------------+----------------------+
    |   gswrap   | 31.36532688140869 s  |          -           |
    | gsutilwrap | 37.959198236465454 s | 1.2102280451272829 x |
    +------------+----------------------+----------------------+

    Benchmark download-many-to-many 500 files:
    +------------+---------------------+----------------------+
    |   Tested   |         Time        |       SpeedUp        |
    +------------+---------------------+----------------------+
    |   gswrap   | 5.657044172286987 s |          -           |
    | gsutilwrap |  66.4119668006897 s | 11.739693871586152 x |
    +------------+---------------------+----------------------+

    Benchmark copy on remote 1000 files:
    +------------+---------------------+----------------------+
    |   Tested   |         Time        |       SpeedUp        |
    +------------+---------------------+----------------------+
    |   gswrap   | 5.135300636291504 s |          -           |
    | gsutilwrap | 4.578975439071655 s | 0.8916664794095477 x |
    +------------+---------------------+----------------------+

    Benchmark copy-many-to-many-on-remote 500 files:
    +------------+---------------------+----------------------+
    |   Tested   |         Time        |       SpeedUp        |
    +------------+---------------------+----------------------+
    |   gswrap   |  6.0890212059021 s  |          -           |
    | gsutilwrap | 70.82826447486877 s | 11.632126425543534 x |
    +------------+---------------------+----------------------+

    Benchmark remove 1000 files:
    +------------+----------------------+----------------------+
    |   Tested   |         Time         |       SpeedUp        |
    +------------+----------------------+----------------------+
    |   gswrap   | 4.313004016876221 s  |          -           |
    | gsutilwrap | 3.3785297870635986 s | 0.7833356458384582 x |
    +------------+----------------------+----------------------+

    Benchmark read 100 files:
    +------------+----------------------+---------------------+
    |   Tested   |         Time         |       SpeedUp       |
    +------------+----------------------+---------------------+
    |   gswrap   | 15.238682746887207 s |          -          |
    | gsutilwrap | 63.807496309280396 s | 4.187205506480821 x |
    +------------+----------------------+---------------------+

    Benchmark write 30 files:
    +------------+----------------------+--------------------+
    |   Tested   |         Time         |      SpeedUp       |
    +------------+----------------------+--------------------+
    |   gswrap   | 2.485429286956787 s  |         -          |
    | gsutilwrap | 26.244182109832764 s | 10.5592149603929 x |
    +------------+----------------------+--------------------+

    Benchmark stat 100 files:
    +------------+---------------------+---------------------+
    |   Tested   |         Time        |       SpeedUp       |
    +------------+---------------------+---------------------+
    |   gswrap   | 5.907729625701904 s |          -          |
    | gsutilwrap | 45.99751901626587 s | 7.785989192218804 x |
    +------------+---------------------+---------------------+


All results of our benchmarks can be found `here
<https://github.com/Parquery/gs-wrap/blob/master/benchmark/benchmark_results>`_.

Versioning
==========
We follow `Semantic Versioning <http://semver.org/spec/v1.0.0.html>`_.
The version X.Y.Z indicates:

* X is the major version (backward-incompatible),
* Y is the minor version (backward-compatible), and
* Z is the patch version (backward-compatible bug fix).
