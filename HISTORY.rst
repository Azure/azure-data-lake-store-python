.. :changelog:

Release History
===============
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