gs-wrap
=======

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

``gs-wrap`` wraps `Google Cloud Storage API <https://cloud.google.com/storage/>`_
for multi-threaded data manipulation including copying, reading, writing and
hashing.

Originally, we used our `gsutilwrap <https://github.com/Parquery/gsutilwrap/>`_,
a thin wrapper around ``gsutil`` command-line interface, to simplify
the deployment and backup tasks related to Google Cloud Storage.
However, ``gsutilwrap`` was prohibitively slow at copying many objects into
different destinations.

Therefore we developed ``gs-wrap`` to accelerate these operations while keeping
it equally fast or faster than ``gsutilwrap`` at other operations.

While the `google-cloud-storage
<https://github.com/googleapis/google-cloud-python/tree/master/storage/>`_
library provided by Google offers sophisticated features and good performance,
its use cases and behavior differ from ``gsutil``. 
Since we wanted the simplicity and usage patterns of ``gsutil``, we created
``gs-wrap``, which wraps ``google-cloud-storage`` in its core and with its
interface set to behave like ``gsutil``.

``gs-wrap`` is not the first Python library wrapping Google Cloud Storage API.
`cloud-storage-client <https://github.com/Rakanixu/cloud-storage-client/>`_
takes a similar approach and aims to manage both Amazon's S3 and Google Cloud
Storage. Parts of it are also based on ``google-cloud-storage``, however the
library's behaviour differs from ``gsutil`` which made it hard to use as an
in-place replacement for ``gsutilwrap``. Additionally, the library did not
offer all needed operations, for example copying to many destinations, reading,
writing and hashing.

The main strength of ``gs-wrap`` is the ability to copy many objects from many
different paths to multiple destinations, while still mimicking ``gsutil``
interface. A direct comparison of performance between ``gs-wrap`` and
``gsutilwrap`` can be found in the `section Benchmarks
<https://github.com/Parquery/gs-wrap#benchmarks>`_.

Usage
=====
You need to create a Google Cloud Storage bucket to use this client library.
Follow along with the `official Google Cloud Storage documentation
<https://cloud.google.com/storage/docs/cloud-console#_creatingbuckets>`_ to
learn how to create a bucket.

Connect to your Google Cloud Storage bucket
-------------------------------------------

First a client for interacting with the Google Cloud Storage API needs to be
created. This one uses internally the `Storage Client
<https://googleapis.github.io/google-cloud-python/latest/storage/client.html#google.cloud.storage.client.Client/>`_
from ``google-cloud-storage``.

One parameter can be passed to the client:

The Google Cloud Storage **project** which the client acts on behalf of. It will
be passed when creating the internal client. If not passed, falls back to the
default inferred from the locally authenticated `Google Cloud SDK
<http://cloud.google.com/sdk>`_ environment. Each project needs a separate
client. Operations between two different projects are not supported.

.. code-block:: python

    import gswrap

    client = gswrap.Client() # project is optional

List objects in your bucket
---------------------------

.. warning::

    Wildcards (\*, \*\*, \?, \[chars\], \[char range\]) are not supported by
    Google Cloud Storage API and neither by ``gs-wrap`` at the moment
    [2019-01-16]. Reasons are that the ``gsutil`` with wildcards can hardly be
    equivalently reconstructed and that the toplevel search is extremely
    inefficient. More information about ``gsutil`` wildcards can be found here:
    `<https://cloud.google.com/storage/docs/gsutil/addlhelp/WildcardNames>`_

.. code-block:: python

    client.ls(gcs_url="gs://your-bucket/your-dir", recursive=False)
    # gs://your-bucket/your-dir/your-subdir1/
    # gs://your-bucket/your-dir/your-subdir2/
    # gs://your-bucket/your-dir/file1

    client.ls(gcs_url="gs://your-bucket/your-dir", recursive=True)
    # gs://your-bucket/your-dir/your-subdir1/file1
    # gs://your-bucket/your-dir/your-subdir1/file2
    # gs://your-bucket/your-dir/your-subdir2/file1
    # gs://your-bucket/your-dir/file1

Copy objects within Google Cloud Storage
----------------------------------------

If both the source and destination URL are cloud URLs from the same provider,
``gsutil`` copies data "in the cloud" (i.e. without downloading to and
uploading from the machine where you run ``gs-wrap``).

.. note::
    client.cp() runs single-threaded by default. When multi-threading is
    activated, the maximum number of workers is the number of processors on the
    machine, multiplied by 5. This is the multi-threading default of the
    `ThreadPoolExecuter from the concurrent.futures library
    <https://docs.python.org/3.5/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_.

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

    # Destination URL without slash
    client.cp(src="gs://your-bucket/some-dir/",
    dst="gs://your-bucket/another-dir", recursive=True)
    # your-bucket after:
    # gs://your-bucket/another-dir/file1
    # gs://your-bucket/another-dir/dir1/file11

    # Destination URL with slash
    client.cp(src="gs://your-bucket/some-dir/",
    dst="gs://your-bucket/another-dir/", recursive=True)
    # your-bucket after:
    # gs://your-bucket/another-dir/some-dir/file1
    # gs://your-bucket/another-dir/some-dir/dir1/file11

    # Choose to copy multi-threaded. (default=False)
    client.cp(src="gs://your-bucket/some-dir/",
    dst="gs://your-bucket/another-dir", recursive=True, multithreaded=True)
    # your-bucket after:
    # gs://your-bucket/another-dir/file1
    # gs://your-bucket/another-dir/dir1/file11

Upload objects to Google Cloud Storage
--------------------------------------

.. note::

    **recursive** causes directories, buckets, and bucket subdirectories to be
    copied recursively. If you upload from local disk to Google Cloud Storage
    and set recursive to ``False``, ``gs-wrap``
    will raise an exception and inform you that no URL matched.
    This mimicks the behaviour of ``gsutil`` when no wildcards are used.

.. code-block:: python

    # Your local directory:
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
    copied recursively. If you upload from local disk to Google Cloud Storage
    and set recursive to ``False``, ``gs-wrap``
    will raise an exception and inform you that no URL matched.
    This mimicks the behaviour of ``gsutil`` when no wildcards are used.

.. code-block:: python

    import os
    # Current your-bucket:
    # gs://your-bucket/file1

    client.cp(
        src="gs://your-bucket/file1", 
        dst="/home/user/storage/file1")

    # Your local directory:
    # /home/user/storage/file1

Copy, download and upload with parameters
-----------------------------------------

.. note::

    All parameters can be used for any kind of ``cp`` operation.

.. code-block:: python

    # Parameter: no_clobber example:
    import os

    # File content before: "hello"
    os.stat("/home/user/storage/file1").st_mtime # 1537947563

    client.cp(
        src="gs://your-bucket/file1",
        dst="/home/user/storage/file1",
        no_clobber=True)

    # no_clobber option stops from overwriting.
    # File content after: "hello"
    os.stat("/home/user/storage/file1").st_mtime # 1537947563

    client.cp(
        src="gs://your-bucket/file1",
        dst="/home/user/storage/file1",
        no_clobber=False)

    # File content after: "hello world"
    os.stat("/home/user/storage/file1").st_mtime # 1540889799

    # Parameter: recursive and multi-threaded example:
    # Your local directory:
    # /home/user/storage/file1
    # ...
    # /home/user/storage/file1000
    # your-bucket before:
    # "empty"

    # Execute normal recursive copy in multiple threads.
    client.cp(src="/home/user/storage/",
              dst="gs://your-bucket/local/",
              recursive=True, multithreaded=True)
    # your-bucket after:
    # gs://your-bucket/local/storage/file1
    # ...
    # gs://your-bucket/local/storage/file1000

    # Parameter: preserve_posix example:
    # Your file before:
    # /home/user/storage/file1
    # e.g. file_mtime: 1547653413 equivalent to 2019-01-16 16:43:33

    client.cp(src="/home/user/storage/file1",
              dst="gs://your-backup-bucket/file1",
              preserve_posix=False)
    # your-backup-bucket after:
    # gs://your-backup-bucket/file1 e.g. "no metadata file_mtime"

    # Preserve the POSIX attributes. POSIX attributes are the metadata of a file.
    client.cp(src="/home/user/storage/file1",
              dst="gs://your-backup-bucket/file1",
              preserve_posix=True)
    # your-backup-bucket after:
    # gs://your-backup-bucket/file1 e.g. file_mtime: 2019-01-16 16:43:33

Perform multiple copy operations in one call
--------------------------------------------

.. code-block:: python

    sources_destinations = [
        # Copy on Google Cloud Storage
        ('gs://your-bucket/your-dir/file',
         'gs://your-bucket/backup-dir/file'),
        
        # Copy from gcs to local
        ('gs://your-bucket/your-dir/file',
         pathlib.Path('/home/user/storage/backup-file')),
        
        # Copy from local to gcs
        (pathlib.Path('/home/user/storage/new-file'),
         'gs://your-bucket/your-dir/new-file'),
        
        # Copy locally
        (pathlib.Path('/home/user/storage/file'),
         pathlib.Path('/home/user/storage/new-file'))]

    client.cp_many_to_many(srcs_dsts=sources_destinations)

Remove files from Google Cloud Storage
--------------------------------------

.. code-block:: python

    # your-bucket before:
    # gs://your-bucket/file
    client.rm(url="gs://your-bucket/file")
    # your-bucket after:
    # "empty"

    # your-bucket before:
    # gs://your-bucket/file1
    # gs://your-bucket/your-dir/file2
    # gs://your-bucket/your-dir/sub-dir/file3
    client.rm(url="gs://your-bucket/your-dir", recursive=True)
    # your-bucket after:
    # gs://your-bucket/file1

Read and write files in Google Cloud Storage
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

    POSIX attributes include meta information about a file. When copying a file
    locally or copying a file within Google Cloud Storage, the POSIX attributes
    are always preserved. On the other hand, when downloading or uploading file
    to Google Cloud Storage, the POSIX attributes is only preserved when
    **preserve_posix** is set to True.

.. code-block:: python

    file = pathlib.Path('/home/user/storage/file')
    file.touch()
    print(file.stat())
    # os.stat_result(st_mode=33204, st_ino=19022665, st_dev=64769, st_nlink=1,
    # st_uid=1000, st_gid=1000, st_size=0, st_atime=1544015997,
    # st_mtime=1544015997, st_ctime=1544015997)

    # Upload does not preserve POSIX attributes.
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

    # Upload with preserve_posix also copy POSIX attributes to blob.
    # POSIX attributes are the metadata of a file.
    # It also works for downloading.

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

    # Check modification time when copied with preserve_posix.
    client.same_modtime(path='/home/user/storage/file',
                        url='gs://your-bucket/file')

    # Check md5 hash to ensure content equality.
    client.same_md5(path='/home/user/storage/file', url='gs://your-bucket/file')

    # Retrieve hex digests of MD5 checksums for multiple URLs.
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

Benchmark list 10000 files:

+------------+--------+---------+
| TESTED     | TIME   | SPEEDUP |
+------------+--------+---------+
| gswrap     | 3.22 s | \-      |
+------------+--------+---------+
| gsutilwrap | 3.98 s | 1.24 x  |
+------------+--------+---------+

Benchmark upload 10000 files:

+------------+---------+---------+
| TESTED     | TIME    | SPEEDUP |
+------------+---------+---------+
| gswrap     | 45.12 s | \-      |
+------------+---------+---------+
| gsutilwrap | 34.85 s | 0.77 x  |
+------------+---------+---------+

Benchmark upload-many-to-many 500 files:

+------------+--------+---------+
| TESTED     | TIME   | SPEEDUP |
+------------+--------+---------+
| gswrap     | 2.14 s | \-      |
+------------+--------+---------+
| gsutilwrap | 65.2 s | 30.49 x |
+------------+--------+---------+

Benchmark download 10000 files:

+------------+---------+---------+
| TESTED     | TIME    | SPEEDUP |
+------------+---------+---------+
| gswrap     | 43.92 s | \-      |
+------------+---------+---------+
| gsutilwrap | 43.01 s | 0.98 x  |
+------------+---------+---------+

Benchmark download-many-to-many 500 files:

+------------+---------+---------+
| TESTED     | TIME    | SPEEDUP |
+------------+---------+---------+
| gswrap     | 5.85 s  | \-      |
+------------+---------+---------+
| gsutilwrap | 62.93 s | 10.76 x |
+------------+---------+---------+

Benchmark copy on remote 1000 files:

+------------+--------+---------+
| TESTED     | TIME   | SPEEDUP |
+------------+--------+---------+
| gswrap     | 5.09 s | \-      |
+------------+--------+---------+
| gsutilwrap | 4.47 s | 0.88 x  |
+------------+--------+---------+

Benchmark copy-many-to-many-on-remote 500 files:

+------------+---------+---------+
| TESTED     | TIME    | SPEEDUP |
+------------+---------+---------+
| gswrap     | 6.55 s  | \-      |
+------------+---------+---------+
| gsutilwrap | 62.76 s | 9.57 x  |
+------------+---------+---------+

Benchmark remove 1000 files:

+------------+--------+---------+
| TESTED     | TIME   | SPEEDUP |
+------------+--------+---------+
| gswrap     | 3.16 s | \-      |
+------------+--------+---------+
| gsutilwrap | 3.66 s | 1.16 x  |
+------------+--------+---------+

Benchmark read 100 files:

+------------+---------+---------+
| TESTED     | TIME    | SPEEDUP |
+------------+---------+---------+
| gswrap     | 16.56 s | \-      |
+------------+---------+---------+
| gsutilwrap | 64.73 s | 3.91 x  |
+------------+---------+---------+

Benchmark write 30 files:

+------------+---------+---------+
| TESTED     | TIME    | SPEEDUP |
+------------+---------+---------+
| gswrap     | 2.67 s  | \-      |
+------------+---------+---------+
| gsutilwrap | 32.55 s | 12.17 x |
+------------+---------+---------+

Benchmark stat 100 files:

+------------+---------+---------+
| TESTED     | TIME    | SPEEDUP |
+------------+---------+---------+
| gswrap     | 6.39 s  | \-      |
+------------+---------+---------+
| gsutilwrap | 48.15 s | 7.53 x  |
+------------+---------+---------+


All results of our benchmarks can be found `here
<https://github.com/Parquery/gs-wrap/blob/master/benchmark/benchmark_results>`_.

Versioning
==========
We follow `Semantic Versioning <http://semver.org/spec/v1.0.0.html>`_.
The version X.Y.Z indicates:

* X is the major version (backward-incompatible),
* Y is the minor version (backward-compatible), and
* Z is the patch version (backward-compatible bug fix).
