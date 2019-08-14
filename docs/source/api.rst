API
===

.. currentmodule:: azure.datalake.store.core

.. autosummary::
    AzureDLFileSystem
    AzureDLFileSystem.access
	AzureDLFileSystem.cat
	AzureDLFileSystem.chmod
	AzureDLFileSystem.chown
	AzureDLFileSystem.concat
	AzureDLFileSystem.cp
	AzureDLFileSystem.df
	AzureDLFileSystem.du
	AzureDLFileSystem.exists
	AzureDLFileSystem.get
	AzureDLFileSystem.get_acl_status
	AzureDLFileSystem.glob
	AzureDLFileSystem.head
	AzureDLFileSystem.info
	AzureDLFileSystem.listdir
	AzureDLFileSystem.ls
	AzureDLFileSystem.merge
	AzureDLFileSystem.mkdir
	AzureDLFileSystem.modify_acl_entries
	AzureDLFileSystem.mv
	AzureDLFileSystem.open
	AzureDLFileSystem.put
	AzureDLFileSystem.remove
	AzureDLFileSystem.remove_acl
	AzureDLFileSystem.remove_acl_entries
	AzureDLFileSystem.remove_default_acl
	AzureDLFileSystem.rename
	AzureDLFileSystem.rm
	AzureDLFileSystem.rmdir
	AzureDLFileSystem.set_acl
	AzureDLFileSystem.set_expiry
	AzureDLFileSystem.stat
	AzureDLFileSystem.tail
	AzureDLFileSystem.touch
	AzureDLFileSystem.unlink
	AzureDLFileSystem.walk

.. autosummary::
   AzureDLFile
   AzureDLFile.close
   AzureDLFile.flush
   AzureDLFile.info
   AzureDLFile.read
   AzureDLFile.seek
   AzureDLFile.tell
   AzureDLFile.write

.. currentmodule:: azure.datalake.store.multithread

.. autosummary::
   ADLUploader
   ADLDownloader

.. currentmodule:: azure.datalake.store.core

.. autoclass:: AzureDLFileSystem
   :members:

.. currentmodule:: azure.datalake.store.multithread

.. autoclass:: ADLUploader
   :members:

.. autoclass:: ADLDownloader
   :members:

.. currentmodule:: azure.datalake.store.lib
.. autofunction:: auth