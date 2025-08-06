package org.eclipse.datagrid.cluster.nodelibrary.common.impl._default;

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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.KafkaException;
import org.eclipse.datagrid.cluster.nodelibrary.common.StorageKafkaReadiness;
import org.eclipse.datagrid.cluster.nodelibrary.common.exception.InternalServerErrorException;
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
	
	private final StorageKafkaReadiness    readiness;
	private final ClusterStorageManager<?> storage;
	
	private Thread garbageCollectionThread;
	
	public DefaultStorageClusterControllerBase(
		final Optional<Supplier<ClusterStorageManager<?>>> clusterStorageManagerSupplier
	) throws KafkaException
	{
		this.storage = clusterStorageManagerSupplier.orElse(
			() -> ServiceLoader.load(ClusterStorageManagerProvider.class)
				.findFirst()
				.get()
				.provideClusterStorageManager()
		).get();
		this.readiness = StorageKafkaReadiness.fromEnv(this.storage);
		this.readiness.init();
	}
	
	@Override
	public boolean distributionActive()
	{
		return this.storage.isDistributor();
	}
	
	@Override
	public void startDistributorActivation()
	{
		this.storage.startDistributionActivation();
	}
	
	@Override
	public boolean finishDistributorActivation()
	{
		return this.storage.finishDistributionActivation();
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
	public boolean isHealthy()
	{
		return this.storage.isRunning() && this.readiness.isActive();
	}
	
	@Override
	public boolean isReady() throws InternalServerErrorException
	{
		try
		{
			return this.readiness.isReady();
		}
		catch (final KafkaException e)
		{
			LOG.error("Failed to check for readiness", e);
			throw new InternalServerErrorException();
		}
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
	public void postStopUpdates()
	{
		if (!ClusterEnv.isBackupNode())
		{
			throw new UnsupportedOperationException("Only Backup Nodes support this feature");
		}
		
		BackupStorage.get().stopAtLatestOffset();
	}
	
	@Override
	public boolean getStopUpdates()
	{
		if (!ClusterEnv.isBackupNode())
		{
			throw new UnsupportedOperationException("Only Backup Nodes support this feature");
		}
		
		return BackupStorage.get().dataClientHasFinished();
	}
	
	@Override
	public void callGc()
	{
		if (this.garbageCollectionThread == null)
		{
			this.garbageCollectionThread = new Thread(
				this.storage::issueFullGarbageCollection,
				"EclipseStore-GarbageCollection-Issuer"
			);
			this.garbageCollectionThread.start();
		}
	}
	
	@Override
	public boolean isGcRunning()
	{
		if (this.garbageCollectionThread != null && !this.garbageCollectionThread.isAlive())
		{
			this.garbageCollectionThread = null;
		}
		return this.garbageCollectionThread != null;
	}
	
	@Override
	public void close()
	{
		try
		{
			this.readiness.close();
		}
		catch (final Exception e)
		{
			LOG.error("Failed to close storage kafka readiness", e);
		}
	}
}
