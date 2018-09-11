.. :changelog:

Release History
===============

0.0.30 (2018-08-28)
+++++++++++++++++++
* Fixed .travis.yml order to add azure-nspg dependency

0.0.29 (2018-08-22)
+++++++++++++++++++
* Fixed HISTORY.rst and Pypi documentation

0.0.28 (2018-08-20)
+++++++++++++++++++
* Added recovery from DatalakeBadOffsetException

0.0.27 (2018-08-08)
+++++++++++++++++++
* Fixed bug in single file check
* Added Python2 exception compatibility

0.0.26 (2018-08-03)
+++++++++++++++++++
* Fixed bug due to not importing errno
* Fixed bug in os.makedirs race condition
* Updated Readme with correct environment variables and fixed some links

0.0.25 (2018-07-26)
+++++++++++++++++++
* Fixed downloading of empty directories and download of directory structure with only a single file

0.0.24 (2018-07-16)
+++++++++++++++++++
* Retry policy implemented for all operations, default being Exponential Retry Policy

0.0.23 (2018-07-11)
+++++++++++++++++++
* Fixed the incorrect download location in case of UNC local paths

0.0.22 (2018-06-02)
+++++++++++++++++++
* Encoding filepaths in URI

0.0.21 (2018-06-01)
+++++++++++++++++++
* Remove unused msrest dependency

0.0.20 (2018-05-25)
+++++++++++++++++++
* Compatibility of the sdist with wheel 0.31.0

0.0.19 (2018-03-14)
-------------------
* Fixed upload issue where destination filename was wrong while upload of directory with single file #208

0.0.18 (2018-02-05)
-------------------
* Fixed read issue where whole file was cached while doing positional reads #198
* Fixed readline as well for the same

0.0.17 (2017-09-21)
-------------------
* Fixed README.rst indentation error
* Changed management endpoint

0.0.16 (2017-09-11)
-------------------
* Fixed Multi chunk transfer hangs as merging chunks fails #187
* Added syncflag and leaseid in create, append calls.
* Added filesessionid in create, append and open calls.

0.0.15 (2017-07-26)
-------------------
* Enable Data Lake Store progress controller callback #174
* Fix File state incorrectly marked as "errored" if contains chunks is "pending" state #182
* Fix Race condition due to `transfer` future `done_callback` #177

0.0.14 (2017-07-10)
-------------------
* Fix an issue where common prefixes in paths for upload and download were collapsed into only unique paths.

0.0.13 (2017-06-28)
-------------------
* Add support for automatic refreshing of service principal credentials

0.0.12 (2017-06-20)
-------------------
* Fix a regression with ls returning the top level folder if it has no contents. It now properly returns an empty array if a folder has no children.

0.0.11 (2017-06-02)
-------------------
* Update to name incomplete file downloads with a `.inprogress` suffix. This suffix is removed when the download completes successfully.

0.0.10 (2017-05-24)
-------------------
* Allow users to explicitly use or invalidate the internal, local cache of the filesystem that is built up from previous `ls` calls. It is now set to always call the service instead of the cache by default.
* Update to properly create the wheel package during build to ensure all pip packages are available.
* Update folder upload/download to properly throw early in the event that the destination files exist and overwrite was not specified. NOTE: target folder existence (or sub folder existence) does not automatically cause failure. Only leaf node existence will result in failure.
* Fix a bug that caused file not found errors when attempting to get information about the root folder.

0.0.9 (2017-05-09)
------------------
* Enforce basic SSL utilization to ensure performance due to `GitHub issue 625 <https://github.com/pyca/pyopenssl/issues/625>`

0.0.8 (2017-04-26)
------------------
* Fix server-side throttling retry support. This is not a guarantee that if the server is throttling the upload (or download) it will eventually succeed, but there is now a back-off retry in place to make it more likely.

0.0.7 (2017-04-19)
------------------
* Update the build process to more efficiently handle multi-part namespaces for pip.

0.0.6 (2017-03-15)
------------------
* Fix an issue with path caching that should drastically improve performance for download

0.0.5 (2017-03-01)
------------------
* Fix for downloader to ensure there is access to the source path before creating destination files
* Fix for credential objects to inherit from msrest.authentication for more universal authentication support
* Add support for the following:

  * set_expiry: allows for setting expiration on files
  * ACL management:

    * set_acl: allows for the full replacement of an ACL on a file or folder
    * set_acl_entries: allows for "patching" an existing ACL on a file or folder
    * get_acl_status: retrieves the ACL information for a file or folder
    * remove_acl_entries: removes the specified entries from an ACL on a file or folder
    * remove_acl: removes all non-default ACL entries from a file or folder
    * remove_default_acl: removes all default ACL entries from a folder

* Remove unsupported and unused "TRUNCATE" operation.
* Added API-Version support with a default of the latest api version (2016-11-01)

0.0.4 (2017-02-07)
------------------
* Fix for folder upload to properly delete folders with contents when overwrite specified.
* Fix to set verbose output to False/Off by default. This removes progress tracking output by default but drastically improves performance.

0.0.3 (2017-02-02)
------------------
* Fix to setup.py to include the HISTORY.rst file. No other changes

0.0.2 (2017-01-30)
------------------
* Addresses an issue with lib.auth() not properly defaulting to 2FA
* Fixes an issue with Overwrite for ADLUploader sometimes not being honored.
* Fixes an issue with empty files not properly being uploaded and resulting in a hang in progress tracking.
* Addition of a samples directory showcasing examples of how to use the client and upload and download logic.
* General cleanup of documentation and comments.
* This is still based on API version 2016-11-01

0.0.1 (2016-11-21)
------------------
* Initial preview release. Based on API version 2016-11-01.
* Includes initial ADLS filesystem functionality and extended upload and download support.
