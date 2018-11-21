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

.. note:: client.cp() always runs multi-threaded.

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

    # choice to copy multi-threaded. (default=1)
    # max_workers can be set to any integer or None.
    # None will choose max_workers equal number of processors times 5.
    client.cp(src="gs://your-bucket/some-dir/",
    dst="gs://your-bucket/another-dir", recursive=True, max_workers=None)
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
