1.0.5
=====
* Changed argument in ``google.cloud.storage.client.Client.get_bucket``
  since interface broke with google-cloud-storage 1.16.0

1.0.4
=====
* ``crc32c`` and ``md5`` in ``Stat`` are decoded into actual bytes.
  Google client sends crc32s and md5 of the objects base-64 encoded which
  is confusing for the end users.

1.0.3
=====
* Added mypy --strict flag in precommit.py and fixed corresponding errors
* Fixed mkdir race condition when downloading
* Updated docstrings with examples
* Re-structured multi-threading execution by adding nested functions and titles
  for readability
* Excluded benchmark module from distribution
* Updated dependencies and re-formatted code for new yapf version

1.0.2
=====
* Fixed downloading a file into a directory bug and added testcase
* Fixed long_ls return type

1.0.1
=====
* Rephrase PyPi description in meta information about gs-wrap package

1.0.0
=====
* Initial version
