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

import org.eclipse.serializer.meta.NotImplementedYetError;

public interface StorageBackupManager extends AutoCloseable
{
    void createStorageBackup();

    @Override
    void close();

    static StorageBackupManager New()
    {
        return new Default();
    }

    class Default implements StorageBackupManager
    {
        private Default()
        {
        }

        @Override
        public void createStorageBackup()
        {
            throw new NotImplementedYetError();
        }

        @Override
        public void close()
        {
            // TODO Auto-generated method stub
        }

        //		public void createBackupNow()
        //		{
        //			this.logger.debug("creating now backup");
        //
        //			final String backupName = Instant.now().toString();
        //
        //			synchronized (BackupStorage.SYNC_KEY)
        //			{
        //				if (ISSUE_BACKUP_OVERWRITE == null)
        //				{
        //					// Ensure a running storage
        //					if (!BackupStorage.isRunning())
        //					{
        //						this.logger.warn("No storage running. Not creating a backup.");
        //						return;
        //					}
        //				}
        //
        //				final String backupFilePath = "/storage/backup/" + System.currentTimeMillis() + ".zip";
        //				final var backupFile = new File(backupFilePath);
        //
        //				try
        //				{
        //					try (final var fs = ZipUtils.createZipFilesystem(backupFilePath))
        //					{
        //						final var nfs = NioFileSystem.New(fs);
        //						final var dir = nfs.ensureDirectory(Paths.get("/storage"));
        //
        //						if (ISSUE_BACKUP_OVERWRITE == null)
        //						{
        //							// TODO: Immediately issue full backup into the s3 bucket instead of the local filesystem
        //							BackupStorage.get().issueFullBackup(dir);
        //						}
        //						else
        //						{
        //							ISSUE_BACKUP_OVERWRITE.accept(dir);
        //						}
        //					}
        //
        //					this.ensureBackupCount(this.keptBackupsCount);
        //					this.client.uploadBackup(backupFile.toPath(), backupName);
        //				}
        //				catch (final IOException | ArchiveException | BackupProxyRequestException | URISyntaxException e)
        //				{
        //					throw new RuntimeException("Failed to create backup.", e);
        //				}
        //				finally
        //				{
        //					if (backupFile.exists())
        //					{
        //						this.logger.info("Cleaning up backup file {}", backupFile);
        //
        //						if (!backupFile.delete())
        //						{
        //							this.logger.warn("Could not clean up backup file {}", backupFile);
        //						}
        //					}
        //
        //					final var backupCreatedPath = Paths.get("/storage/backup_created");
        //					if (Files.notExists(backupCreatedPath))
        //					{
        //						try
        //						{
        //							Files.createFile(backupCreatedPath);
        //						}
        //						catch (final IOException e)
        //						{
        //							throw new RuntimeException("Failed to create backup created indicator file.", e);
        //						}
        //					}
        //				}
        //			}
        //		}
        //
        //		public void ensureBackupCount(final int keptBackupsCount)
        //		{
        //			final List<BackupListItem> backupList = this.client.listBackups();
        //
        //			if (backupList.size() == 0 || backupList.size() < keptBackupsCount)
        //			{
        //				return;
        //			}
        //
        //			final BackupListItem oldestBackup = backupList.stream()
        //				.map(b -> Pair.of(Instant.parse(b.getName()), b))
        //				.sorted((a, b) -> b.left().compareTo(a.left()))
        //				.map(b -> b.right())
        //				.findFirst()
        //				.get();
        //
        //			this.client.deleteBackup(oldestBackup.getName());
        //		}
    }
}
