package org.eclipse.datagrid.cluster.nodelibrary.types;

import static org.eclipse.serializer.util.X.notNull;

import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface StorageBackupTaskExecutor extends StorageTaskExecutor
{
	void runBackup(boolean useManualSlot);

	boolean isRunningBackup();

	static StorageBackupTaskExecutor New(final StorageConnection connection, final StorageBackupManager backupManager)
	{
		return new Default(notNull(connection), notNull(backupManager));
	}

	final class Default extends StorageTaskExecutor.Abstract implements StorageBackupTaskExecutor
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageBackupTaskExecutor.class);
		private final StorageBackupManager backupManager;

		private Thread backupThread;

		private Default(final StorageConnection connection, final StorageBackupManager backupManager)
		{
			super(connection);
			this.backupManager = backupManager;
		}

		@Override
		public void runBackup(final boolean useManualSlot)
		{
			if (this.backupThread == null || !this.backupThread.isAlive())
			{
				LOG.debug("Issuing new storage backup");
				this.backupThread = new Thread(
					() -> this.backupManager.createStorageBackup(useManualSlot),
					"EclipseStore-StorageBackup"
				);
				this.backupThread.start();
			}
		}

		@Override
		public boolean isRunningBackup()
		{
			if (this.backupThread != null && !this.backupThread.isAlive())
			{
				LOG.trace("Cleanup previous storage backup thread");
				this.backupThread = null;
			}
			return this.backupThread != null;
		}
	}
}
