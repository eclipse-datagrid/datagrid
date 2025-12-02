package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 * 
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConnection;

import static org.eclipse.serializer.util.X.notNull;

public interface FilesystemVolumeBackupBackend extends StorageBackupBackend
{
	static FilesystemVolumeBackupBackend New(
		final Path backupVolumePath,
		final StoredMessageIndexManager.Creator storedmessageIndexManagerCreator
	)
	{
		return new Default(notNull(backupVolumePath), notNull(storedmessageIndexManagerCreator));
	}

	static class Default implements FilesystemVolumeBackupBackend
	{
		private final Path backupVolumePath;
		private final Path userUploadedStorageFolderPath;
		private final StoredMessageIndexManager.Creator messageIndexManagerCreator;

		private Default(
			final Path backupVolumePath,
			final StoredMessageIndexManager.Creator storedMessageIndexManagerCreator
		)
		{
			this.backupVolumePath = backupVolumePath;
			this.userUploadedStorageFolderPath = backupVolumePath.resolve("user-uploaded-storage");
			this.messageIndexManagerCreator = storedMessageIndexManagerCreator;
		}

		@Override
		public List<BackupMetadata> listBackups() throws NodelibraryException
		{
			return this.listBackupVolumeFiles()
				.stream()
				.filter(f -> !f.equals(this.userUploadedStorageFolderPath.getFileName().toString()))
				.map(this::parseMetadata)
				.collect(Collectors.toList());
		}

		@Override
		public void deleteBackup(final BackupMetadata backup) throws NodelibraryException
		{
			this.deleteDirectory(this.toBackupFolderPath(backup));
		}

		@Override
		public void createAndUploadBackup(
			final StorageConnection connection,
			final MessageInfo messageInfo,
			final BackupMetadata backup
		) throws NodelibraryException
		{
			final Path backupRootPath = this.toBackupFolderPath(backup);
			final var fs = Storage.DefaultFileSystem();

			connection.issueFullBackup(fs.ensureDirectory(backupRootPath.resolve("storage")));
			// TODO: Hardcoded offset file name
			try (
				final var infoWriter = this.messageIndexManagerCreator.create(
					fs.ensureFile(backupRootPath.resolve("offset")).tryUseWriting()
				)
			)
			{
				infoWriter.set(messageInfo);
			}
			try
			{
				Files.createFile(backupRootPath.resolve("ready"));
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to create ready file");
			}
		}

		@Override
		public void downloadBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
			throws NodelibraryException
		{
			final Path backupRootPath = this.toBackupFolderPath(backup);
			this.copyDirectory(backupRootPath, storageDestinationParentPath);
		}

		@Override
		public boolean hasUserUploadedStorage() throws NodelibraryException
		{
			return this.listBackupVolumeFiles()
				.stream()
				.anyMatch(f -> f.equals(this.userUploadedStorageFolderPath.getFileName().toString()));
		}

		@Override
		public void downloadUserUploadedStorage(final Path storageDestinationParentPath) throws NodelibraryException
		{
			this.copyDirectory(this.userUploadedStorageFolderPath, storageDestinationParentPath);
		}

		@Override
		public void deleteUserUploadedStorage() throws NodelibraryException
		{
			this.deleteDirectory(this.userUploadedStorageFolderPath);
		}

		@Override
		public void downloadLatestBackup(final Path targetRootPath) throws NodelibraryException
		{
			final var backup = this.latestBackup(false);
			if (backup == null)
			{
				throw new NodelibraryException("No backups are available to download");
			}
			this.downloadBackup(targetRootPath, backup);
		}

		private Path toBackupFolderPath(final BackupMetadata backup)
		{
			return this.backupVolumePath.resolve(
				Long.toString(backup.timestamp()) + (backup.manualSlot() ? ".manual" : "")
			);
		}

		private BackupMetadata parseMetadata(final String folderName) throws NodelibraryException
		{
			final String manualSuffix = ".manual";
			final boolean isManual = folderName.endsWith(manualSuffix);
			final long timestamp;
			final String backupName = isManual ? folderName.substring(0, folderName.length() - manualSuffix.length())
				: folderName;
			try
			{
				timestamp = Long.parseLong(backupName);
			}
			catch (final NumberFormatException e)
			{
				throw new NodelibraryException(
					"Failed to parse backup timestamp for backup " + this.backupVolumePath.resolve(folderName)
				);
			}
			return new BackupMetadata(timestamp, isManual);
		}

		private void deleteDirectory(final Path path) throws NodelibraryException
		{
			try (final var directories = Files.walk(path))
			{
				directories.sorted(Comparator.reverseOrder()).forEach(f ->
				{
					try
					{
						Files.delete(f);
					}
					catch (final IOException e)
					{
						throw new NodelibraryException("Failed to delete file at " + f.toString(), e);
					}
				});
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to iterate files at " + path);
			}
		}

		private void copyDirectory(final Path sourceDir, final Path destinationDir)
		{
			try (final var storageFiles = Files.walk(sourceDir);)
			{
				storageFiles.forEach(f ->
				{
					try
					{
						Files.copy(f, destinationDir.resolve(sourceDir.relativize(f)));
					}
					catch (final IOException e)
					{
						throw new NodelibraryException("Failed to copy file", e);
					}
				});
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to walk files at " + sourceDir, e);
			}
		}

		private List<String> listBackupVolumeFiles() throws NodelibraryException
		{
			try (final var listStream = Files.list(this.backupVolumePath))
			{
				return listStream.map(p -> p.getFileName().toString()).collect(Collectors.toList());
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to iterate backup files at " + this.backupVolumePath);
			}
		}
	}
}
