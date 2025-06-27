package org.eclipse.datagrid.cluster.nodelibrary.common.impl._default;

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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
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

public class DefaultStorageClusterControllerBase implements StorageClusterControllerBase.Impl
{
	private static final Logger LOG = LoggerFactory.getLogger(StorageClusterControllerBase.class);

	private final ClusterStorageManager<?> storage;

	public DefaultStorageClusterControllerBase(
		final Optional<Supplier<ClusterStorageManager<?>>> clusterStorageManagerSupplier
	)
	{
		this.storage = clusterStorageManagerSupplier.orElse(
			() -> ServiceLoader.load(ClusterStorageManagerProvider.class)
				.findFirst()
				.get()
				.provideClusterStorageManager()
		).get();
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
		if (!ClusterEnv.isBackupNode())
		{
			throw new UnsupportedOperationException("Only Backup Nodes support this feature");
		}

		synchronized (BackupStorage.SYNC_KEY)
		{
			final var storagePath = Paths.get("/storage/storage");

			BackupStorage.stop();

			if (Files.isDirectory(storagePath))
			{
				LOG.info("Deleting {}", storagePath);
				FileUtils.deleteDirectory(storagePath.toFile());
			}

			GzipUtils.extractTarGZ(storage);

			BackupStorage.get();
			LOG.info("Backup node is now running the new storage.");
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
		if (!ClusterEnv.isBackupNode())
		{
			throw new UnsupportedOperationException("Only Backup Nodes support this feature");
		}

		final StorageBackupManager backupManager = BackupStorage.getStorageBackupManager();
		if (backupManager == null)
		{
			throw new NullPointerException("no running backup manager found");
		}
		backupManager.createBackupNow();
	}

	@Override
	public void stopUpdates()
	{
		if (!ClusterEnv.isBackupNode())
		{
			throw new UnsupportedOperationException("Only Backup Nodes support this feature");
		}

		BackupStorage.get().stopAtLatestOffset();
	}

	@Override
	public void callGc()
	{
		this.storage.issueFullGarbageCollection();
	}
}
