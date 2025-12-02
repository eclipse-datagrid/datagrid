package org.eclipse.datagrid.cluster.nodelibrary.types;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.concurrency.XThreads;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.math.XMath.positive;

import static org.eclipse.serializer.util.X.notNull;

public interface StorageBackupManager
{
	void createStorageBackup(boolean useManualSlot) throws NodelibraryException;

	void downloadLatestBackup(Path targetRootPath) throws NodelibraryException;

	List<BackupMetadata> listBackups() throws NodelibraryException;

	default BackupMetadata latestBackup(final boolean ignoreManualSlot) throws NodelibraryException
	{
		return this.listBackups()
			.stream()
			.filter(b -> !ignoreManualSlot || !b.manualSlot())
			.max((a, b) -> Long.compare(a.timestamp(), b.timestamp()))
			.orElse(null);
	}

	void deleteBackup(BackupMetadata backup) throws NodelibraryException;

	void downloadBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodelibraryException;

	boolean hasUserUploadedStorage() throws NodelibraryException;

	void downloadUserUploadedStorage(Path storageDestinationParentPath) throws NodelibraryException;

	void deleteUserUploadedStorage() throws NodelibraryException;

	static StorageBackupManager New(
		final StorageConnection storageConnection,

		final int maxBackupCount,
		final StorageBackupBackend storageBackupBackend,
		final Supplier<MessageInfo> messageInfoSupplier,
		final ClusterStorageBinaryDataClient dataClient
	)
	{
		return new Default(
			notNull(storageConnection),
			positive(maxBackupCount),
			notNull(storageBackupBackend),
			notNull(messageInfoSupplier),
			notNull(dataClient)
		);
	}

	class Default implements StorageBackupManager
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageBackupManager.class);

		private final StorageConnection storageConnection;
		private final int maxBackupCount;
		private final StorageBackupBackend backend;
		private final Supplier<MessageInfo> messageInfoSupplier;
		private final ClusterStorageBinaryDataClient dataClient;

		private Default(
			final StorageConnection storageConnection,

			final int maxBackupCount,
			final StorageBackupBackend backupBackend,
			final Supplier<MessageInfo> messageInfoSupplier,
			final ClusterStorageBinaryDataClient dataClient
		)
		{
			this.storageConnection = storageConnection;
			this.maxBackupCount = maxBackupCount;
			this.backend = backupBackend;
			this.messageInfoSupplier = messageInfoSupplier;
			this.dataClient = dataClient;
		}

		@Override
		public void createStorageBackup(final boolean useManualSlot) throws NodelibraryException
		{
			LOG.trace("Creating new storage backup");

			final var newBackup = new BackupMetadata(System.currentTimeMillis(), useManualSlot);
			final List<BackupMetadata> backups = this.listBackups();

			if (!backups.isEmpty())
			{
				if (useManualSlot)
				{
					backups.stream().filter(BackupMetadata::manualSlot).forEach(this::deleteBackup);
				}
				else
				{
					// just in case there are multiple backups too many
					final int toDeleteCount = (int)backups.stream().filter(b -> !b.manualSlot()).count()
						- this.maxBackupCount + 1;
					LOG.debug("Deleting {} oldest backup(s)", toDeleteCount);
					for (int i = 0; i < toDeleteCount; i++)
					{
						this.deleteBackup(this.oldestBackup(backups));
					}
				}
			}

			this.stopDataClient();

			this.backend.createAndUploadBackup(this.storageConnection, this.messageInfoSupplier.get(), newBackup);

			this.dataClient.resume();
		}

		@Override
		public void downloadLatestBackup(final Path targetRootPath)
		{
			final var backup = this.latestBackup(false);
			if (backup == null)
			{
				throw new NodelibraryException("No backups are available to download");
			}
			this.downloadBackup(targetRootPath, backup);
		}

		@Override
		public void deleteBackup(final BackupMetadata backup) throws NodelibraryException
		{
			this.backend.deleteBackup(backup);
		}

		@Override
		public void deleteUserUploadedStorage() throws NodelibraryException
		{
			this.backend.deleteUserUploadedStorage();
		}

		@Override
		public void downloadBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
			throws NodelibraryException
		{
			this.backend.downloadBackup(storageDestinationParentPath, backup);
		}

		@Override
		public void downloadUserUploadedStorage(final Path storageDestinationParentPath) throws NodelibraryException
		{
			this.backend.downloadUserUploadedStorage(storageDestinationParentPath);
		}

		@Override
		public boolean hasUserUploadedStorage() throws NodelibraryException
		{
			return this.backend.hasUserUploadedStorage();
		}

		@Override
		public List<BackupMetadata> listBackups() throws NodelibraryException
		{
			return this.backend.listBackups();
		}

		private void stopDataClient()
		{
			LOG.trace("Waiting for data client to stop reading");
			this.dataClient.stopAtLatestMessage();
			while (this.dataClient.isRunning())
			{
				XThreads.sleep(500);
			}
		}

		private BackupMetadata oldestBackup(final List<BackupMetadata> backups)

		{
			return backups.stream().min((a, b) -> Long.compare(a.timestamp(), b.timestamp())).orElse(null);
		}
	}
}
