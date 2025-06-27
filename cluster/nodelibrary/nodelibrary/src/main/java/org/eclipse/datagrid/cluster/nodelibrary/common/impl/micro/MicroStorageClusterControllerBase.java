package org.eclipse.datagrid.cluster.nodelibrary.common.impl.micro;

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
import java.io.InputStream;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.StorageClusterControllerBase;
import org.eclipse.datagrid.cluster.nodelibrary.common.backup.BackupStorage;
import org.eclipse.datagrid.cluster.nodelibrary.common.backup.StorageBackupManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.spi.ClusterStorageManagerProvider;
import org.eclipse.datagrid.cluster.nodelibrary.common.util.GzipUtils;

public class MicroStorageClusterControllerBase implements StorageClusterControllerBase.Impl
{
	private final Logger logger = LoggerFactory.getLogger(MicroStorageClusterControllerBase.class);
	private final ClusterStorageManager<?> storage;
	private final StorageBackupManager backupManager;

	public MicroStorageClusterControllerBase(
		final Optional<Supplier<ClusterStorageManager<?>>> clusterStorageManagerSupplier
	)
	{
		this.storage = clusterStorageManagerSupplier.orElse(
			() -> ServiceLoader.load(ClusterStorageManagerProvider.class)
				.findFirst()
				.get()
				.provideClusterStorageManager()
		).get();

		final Integer keptBackups = ClusterEnv.keptBackupsCount();

		if (keptBackups != null)
		{
			try
			{
				StorageBackupManager.ISSUE_BACKUP_OVERWRITE = this.storage::issueFullBackup;
				this.backupManager = new StorageBackupManager(keptBackups);
			}
			catch (final NumberFormatException e)
			{
				throw new RuntimeException(e);
			}
		}
		else
		{
			this.backupManager = null;
		}
	}

	@Override
	public boolean distributionActive()
	{
		return this.storage.isDistributor();
	}

	@Override
	public void activateDistributor()
	{
		this.storage.activateDistribution();
	}

	@Override
	public void uploadStorage(final InputStream storage) throws IOException
	{
		final var storagePath = new File("/storage/storage");

		synchronized (BackupStorage.SYNC_KEY)
		{
			if (storagePath.isDirectory())
			{
				this.logger.info("Deleting {}", storagePath);
				FileUtils.cleanDirectory(storagePath);
			}

			GzipUtils.extractTarGZ(storage);
		}
	}

	@Override
	public boolean isReady()
	{
		return this.storage.isReady();
	}

	@Override
	public void createBackupNow()
	{
		this.backupManager.createBackupNow();
	}

	@Override
	public void stopUpdates()
	{
		throw new UnsupportedOperationException("Unsupported logic for micro nodes");
	}

	@Override
	public void callGc()
	{
		throw new UnsupportedOperationException("Unsupported logic for micro nodes");
	}
}
