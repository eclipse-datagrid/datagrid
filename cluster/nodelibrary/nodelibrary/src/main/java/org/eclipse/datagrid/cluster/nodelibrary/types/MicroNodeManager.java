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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.meta.NotImplementedYetError;
import org.eclipse.store.storage.types.StorageController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static org.eclipse.serializer.util.X.notNull;

public interface MicroNodeManager extends ClusterNodeManager
{
	void createStorageBackup() throws NodelibraryException;

	boolean isBackupRunning();

	boolean replaceStorage(InputStream storageStream);

	static MicroNodeManager New(
		final StorageTaskExecutor storageTaskExecutor,
		final StorageController storageController,
		final StorageBackupManager backupManager,
		final StorageDiskSpaceReader storageDiskSpaceReader
	)
	{
		return new Default(
			notNull(storageTaskExecutor),
			notNull(storageController),
			notNull(backupManager),
			notNull(storageDiskSpaceReader)
		);
	}

	final class Default implements MicroNodeManager
	{
		private static final Logger LOG = LoggerFactory.getLogger(MicroNodeManager.class);

		private final StorageTaskExecutor tasks;
		private final StorageController storageController;
		private final StorageBackupManager backupManager;
		private final StorageDiskSpaceReader storageDiskSpaceReader;

		private Default(
			StorageTaskExecutor storageTaskExecutor,
			final StorageController storageController,
			final StorageBackupManager backupManager,
			final StorageDiskSpaceReader storageDiskSpaceReader
		)
		{
			this.tasks = storageTaskExecutor;
			this.storageController = storageController;
			this.backupManager = backupManager;
			this.storageDiskSpaceReader = storageDiskSpaceReader;
		}

		@Override
		public void close()
		{
			LOG.info("Closing MicroNodeManager.");
			//this.backupManager.close();
		}

		@Override
		public void createStorageBackup() throws NodelibraryException
		{
			this.backupManager.createStorageBackup(false);
		}

		@Override
		public boolean isBackupRunning()
		{
			// creating backups on micro nodes is not concurrent. So it's always done when calling this
			return false;
		}

		@Override
		public boolean isHealthy()
		{
			return this.storageController.isRunning() && !this.storageController.isStartingUp();
		}

		@Override
		public boolean isReady() throws NodelibraryException
		{
			return this.storageController.isRunning() && !this.storageController.isStartingUp();
		}

		@Override
		public boolean isRunningStorageChecks()
		{
			return this.tasks.isRunningChecks();
		}

		@Override
		public long readStorageSizeBytes() throws NodelibraryException
		{
			return this.storageDiskSpaceReader.readUsedDiskSpaceBytes();
		}

		@Override
		public boolean replaceStorage(final InputStream storageStream)
		{
			// TODO Implement
			throw new NotImplementedYetError();
		}

		@Override
		public void startStorageChecks()
		{
			this.tasks.runChecks();
		}
	}
}
