package org.eclipse.datagrid.cluster.nodelibrary.common.backup;

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
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;

import org.eclipse.serializer.afs.types.ADirectory;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.embedded.configuration.types.EmbeddedStorageConfigurationBuilder;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.storage.MyStorageBinaryDataClientKafka;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMerger;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAcceptor;

public class BackupStorage implements AutoCloseable
{
	public static final Object SYNC_KEY = new Object();
	private static final Logger LOG = LoggerFactory.getLogger(BackupStorage.class);
	private static final Path STORAGE_PATH = Paths.get("/storage/storage");
	
	private static Supplier<?> startRootSupplier;
	private static long        offset = Long.MIN_VALUE;
	private static BackupStorage instance;
	private static StorageBackupManager backupManager;
	private static EmbeddedStorageConfigurationBuilder backupStorageManagerConfig;

	public static void setBackupStorageManagerConfig(final EmbeddedStorageConfigurationBuilder config)
	{
		backupStorageManagerConfig = config;
	}
	
	public static void setRootSupplier(final Supplier<?> rootSupplier)
	{
		startRootSupplier = rootSupplier;
	}

	public static BackupStorage get()
	{
		if (instance == null)
		{
			restart();
		}

		return instance;
	}

	public static boolean isRunning()
	{
		return instance != null;
	}

	public static void restart()
	{
		if (instance != null)
		{
			instance.close();
		}

		instance = new BackupStorage();
	}

	private static void startBackupManager(final int keptBackupsCount)
	{
		if (backupManager == null)
		{
			backupManager = new StorageBackupManager(keptBackupsCount);
		}
	}

	public static StorageBackupManager getStorageBackupManager()
	{
		return backupManager;
	}

	/**
	 * Halts the current storage and returns the current EclipseStore offset
	 */
	public static long stop()
	{
		// Make sure that the storage started at least once
		get();

		backupManager = null;

		LOG.info("Stopping BackupStorage");

		instance.dataClient.dispose();

		while (!instance.dataClient.isFinished())
		{
			try
			{
				Thread.sleep(10);
			}
			catch (final InterruptedException e)
			{
				throw new RuntimeException(e);
			}
		}

		final long offset = instance.dataClient.getStorageOffset();

		instance.close();
		instance = null;

		BackupStorage.offset = offset;

		return offset;
	}

	public static Path getStoragePath()
	{
		return STORAGE_PATH;
	}

	private final EmbeddedStorageManager storage;
	private final MyStorageBinaryDataClientKafka dataClient;

	private BackupStorage()
	{
		LOG.info("Starting db.");

		loadOffset();

		final EmbeddedStorageFoundation<?> foundation = backupStorageManagerConfig.setStorageDirectory(
			STORAGE_PATH.toString()
		).createEmbeddedStorageFoundation();

		this.storage = foundation.start();

		if (this.storage.root() == null)
		{
			Object root = startRootSupplier.get();
			if (!(root instanceof Lazy))
			{
				root = Lazy.Reference(root);
			}
			if (root == null)
			{
				throw new RuntimeException("No root has been set before initialization.");
			}
			this.storage.setRoot(root);
			this.storage.storeRoot();
		}

		final String topic = ClusterEnv.kafkaTopicName();

		this.dataClient = new MyStorageBinaryDataClientKafka(
			topic,
			topic + "-backup",
			offset,
			StorageBinaryDataPacketAcceptor.New(
				StorageBinaryDataMerger.New(
					foundation.getConnectionFoundation(),
					this.storage,
					ObjectGraphUpdateHandler.Synchronized()
				)
			)
		);

		loadOffset();
		LOG.info("Startin kafka client with offset {}.", offset);
		this.dataClient.start();

		Runtime.getRuntime().addShutdownHook(new Thread(BackupStorage::stop, "StopMsBackupStorage"));

		final Integer keptBackups = ClusterEnv.keptBackupsCount();
		if (keptBackups != null)
		{
			try
			{
				startBackupManager(keptBackups);
			}
			catch (final NumberFormatException e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	public boolean issueGarbageCollection(final long nanoTimeBudget)
	{
		return this.storage.issueGarbageCollection(nanoTimeBudget);
	}

	public void issueFullBackup(final ADirectory path)
	{
		this.storage.issueFullBackup(path);
	}

	public void issueFullFileCheck()
	{
		this.storage.issueFullFileCheck();
	}

	public void stopAtLatestOffset()
	{
		this.dataClient.stopAtLatestOffset();
	}

	@Override
	public void close()
	{
		LOG.info("Closing db.");
		this.dataClient.dispose();
		this.storage.close();
	}

	static void loadOffset()
	{
		final var offsetPath = Paths.get("/storage/offset");

		try
		{
			if (Files.exists(offsetPath))
			{
				offset = Long.parseLong(Files.readString(offsetPath));
				LOG.info("Loaded EclipseStore offset {}", offset);
			}
			else
			{
				offset = Long.MIN_VALUE;
				LOG.info("Setting EclipseStore offset to default value {}", offset);
				Files.writeString(offsetPath, Long.toString(offset), StandardOpenOption.CREATE);
			}
		}
		catch (NumberFormatException | IOException e)
		{
			LOG.warn("Failed to load EclipseStore offset from file", e);
		}
	}
	
}
