gswrap
======

Python3 wrapper for gsutil commands

Usage
=====

* Connect to your Google Cloud Storage bucket

.. code-block:: python

    import gswrap

    client = gswrap.Client()

* List objects in your bucket

.. code-block:: python

    client.ls(gcs_url="gs://your-bucket/your-dir", recursive=False)
    # gs://your-bucket/your-dir/your-subdir1
    # gs://your-bucket/your-dir/your-subdir2
    # gs://your-bucket/your-dir/file1

    client.ls(gcs_url="gs://your-bucket/your-dir", recursive=True)
    # gs://your-bucket/your-dir/your-subdir1/file1
    # gs://your-bucket/your-dir/your-subdir2/file1
    # gs://your-bucket/your-dir/file1

* Copy objects in Google Cloud Storage

.. code-block:: python

    client.cp(src="gs://your-bucket/file1", dst="gs://your-bucket/your-dir/",
    recursive=True)
    # client.ls("gs://your-bucket/", recursive=True):
    # gs://your-bucket/file1
    # gs://your-bucket/your-dir/file1

    client.cp(src="gs://your-bucket/file1", dst="gs://your-backup-bucket/backup-file1",
    recursive=False)
    # client.ls("gs://your-backup-bucket/"):
    # gs://your-backup-bucket/backup-file1

* Upload objects to Google Cloud Storage

.. code-block:: python

    # local directory:
    # /home/user/storage/file1
    # /home/user/storage/file2

    client.cp(src="/home/user/storage/", dst="gs://your-bucket/local/",
    recursive=True)
    # client.ls("gs://your-bucket/", recursive=True):
    # gs://your-bucket/local/storage/file1
    # gs://your-bucket/local/storage/file2

* Download objects from Google Cloud Storage

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

We use tox for testing and packaging the distribution. Assuming that the above-mentioned environment variables has been set, the virutal environment has been activated and the development dependencies have been installed, run:

.. code-block:: bash

    tox


Pre-commit Checks
-----------------

We provide a set of pre-commit checks that lint and check code for formatting.

Namely, we use:

* `yapf <https://github.com/google/yapf>`_ to check the formatting.
* The style of the docstrings is checked with `pydocstyle <https://github.com/PyCQA/pydocstyle>`_.
* Static type analysis is performed with `mypy <http://mypy-lang.org/>`_.
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
