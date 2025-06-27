package org.eclipse.datagrid.cluster.nodelibrary.common.backup;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary
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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import org.eclipse.serializer.afs.types.ADirectory;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.datagrid.cluster.nodelibrary.common.exception.ArchiveException;
import org.eclipse.datagrid.cluster.nodelibrary.common.exception.BackupProxyRequestException;
import org.eclipse.datagrid.cluster.nodelibrary.common.util.Pair;
import org.eclipse.datagrid.cluster.nodelibrary.common.util.ZipUtils;

public class StorageBackupManager
{
	/**
	 * Used by the micro deployment to use the single storage node as the backup
	 * node.
	 */
	public static Consumer<ADirectory> ISSUE_BACKUP_OVERWRITE;

	private final Logger logger = LoggerFactory.getLogger(StorageBackupManager.class);
	private final BackupProxyClient client = new BackupProxyClient();

	private final int keptBackupsCount;

	public StorageBackupManager(final int keptBackupsCount)
	{
		this.keptBackupsCount = keptBackupsCount;
	}

	public void createBackupNow()
	{
		this.logger.debug("creating now backup");

		final String backupName = Instant.now().toString();

		synchronized (BackupStorage.SYNC_KEY)
		{
			if (ISSUE_BACKUP_OVERWRITE == null)
			{
				// Ensure a running storage
				if (!BackupStorage.isRunning())
				{
					this.logger.warn("No storage running. Not creating a backup.");
					return;
				}
			}

			final String backupFilePath = "/storage/backup/" + System.currentTimeMillis() + ".zip";
			final var backupFile = new File(backupFilePath);

			try
			{
				try (final var fs = ZipUtils.createZipFilesystem(backupFilePath))
				{
					final var nfs = NioFileSystem.New(fs);
					final var dir = nfs.ensureDirectory(Paths.get("/storage"));

					if (ISSUE_BACKUP_OVERWRITE == null)
					{
						// TODO: Immediately issue full backup into the s3 bucket instead of the local filesystem
						BackupStorage.get().issueFullBackup(dir);
					}
					else
					{
						ISSUE_BACKUP_OVERWRITE.accept(dir);
					}
				}

				this.ensureBackupCount(this.keptBackupsCount);
				this.client.uploadBackup(backupFile.toPath(), backupName);
			}
			catch (final IOException | ArchiveException | BackupProxyRequestException | URISyntaxException e)
			{
				throw new RuntimeException("Failed to create backup.", e);
			}
			finally
			{
				if (backupFile.exists())
				{
					this.logger.info("Cleaning up backup file {}", backupFile);

					if (!backupFile.delete())
					{
						this.logger.warn("Could not clean up backup file {}", backupFile);
					}
				}

				final var backupCreatedPath = Paths.get("/storage/backup_created");
				if (Files.notExists(backupCreatedPath))
				{
					try
					{
						Files.createFile(backupCreatedPath);
					}
					catch (final IOException e)
					{
						throw new RuntimeException("Failed to create backup created indicator file.", e);
					}
				}
			}
		}
	}

	public void ensureBackupCount(final int keptBackupsCount)
	{
		final List<BackupListItem> backupList = this.client.listBackups();

		if (backupList.size() == 0 || backupList.size() < keptBackupsCount)
		{
			return;
		}

		final BackupListItem oldestBackup = backupList.stream()
			.map(b -> Pair.of(Instant.parse(b.getName()), b))
			.sorted((a, b) -> b.left().compareTo(a.left()))
			.map(b -> b.right())
			.findFirst()
			.get();

		this.client.deleteBackup(oldestBackup.getName());
	}
}
