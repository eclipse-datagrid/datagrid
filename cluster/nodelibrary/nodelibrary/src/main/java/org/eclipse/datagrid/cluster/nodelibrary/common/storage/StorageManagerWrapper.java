package org.eclipse.datagrid.cluster.nodelibrary.common.storage;

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

import java.nio.ByteBuffer;
import java.util.function.Predicate;

import org.eclipse.serializer.afs.types.AFile;
import org.eclipse.serializer.collections.types.XGettingEnum;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.serializer.persistence.types.PersistenceRootsView;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;
import org.eclipse.store.storage.types.Database;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.eclipse.store.storage.types.StorageConnection;
import org.eclipse.store.storage.types.StorageEntityCacheEvaluator;
import org.eclipse.store.storage.types.StorageEntityTypeExportFileProvider;
import org.eclipse.store.storage.types.StorageEntityTypeExportStatistics;
import org.eclipse.store.storage.types.StorageEntityTypeHandler;
import org.eclipse.store.storage.types.StorageLiveFileProvider;
import org.eclipse.store.storage.types.StorageManager;
import org.eclipse.store.storage.types.StorageRawFileStatistics;
import org.eclipse.store.storage.types.StorageTypeDictionary;

public abstract class StorageManagerWrapper implements StorageManager
{
	protected abstract StorageManager getWrappedStorageManager();

	@Override
	public StorageConfiguration configuration()
	{
		return this.getWrappedStorageManager().configuration();
	}

	@Override
	public StorageTypeDictionary typeDictionary()
	{
		return this.getWrappedStorageManager().typeDictionary();
	}

	@Override
	public StorageManager start()
	{
		this.getWrappedStorageManager().start();
		return this;
	}

	@Override
	public boolean shutdown()
	{
		return this.getWrappedStorageManager().shutdown();
	}

	@Override
	public StorageConnection createConnection()
	{
		return this.getWrappedStorageManager().createConnection();
	}

	@Override
	public Object root()
	{
		return this.getWrappedStorageManager().root();
	}

	@Override
	public Object setRoot(final Object newRoot)
	{
		return this.getWrappedStorageManager().setRoot(newRoot);
	}

	@Override
	public long storeRoot()
	{
		return this.getWrappedStorageManager().storeRoot();
	}

	@Override
	public PersistenceRootsView viewRoots()
	{
		return this.getWrappedStorageManager().viewRoots();
	}

	@Override
	public Database database()
	{
		return this.getWrappedStorageManager().database();
	}

	@Override
	public boolean isAcceptingTasks()
	{
		return this.getWrappedStorageManager().isAcceptingTasks();
	}

	@Override
	public boolean isRunning()
	{
		return this.getWrappedStorageManager().isRunning();
	}

	@Override
	public boolean isStartingUp()
	{
		return this.getWrappedStorageManager().isStartingUp();
	}

	@Override
	public boolean isShuttingDown()
	{
		return this.getWrappedStorageManager().isShuttingDown();
	}

	@Override
	public void checkAcceptingTasks()
	{
		this.getWrappedStorageManager().checkAcceptingTasks();
	}

	@Override
	public long initializationTime()
	{
		return this.getWrappedStorageManager().initializationTime();
	}

	@Override
	public long operationModeTime()
	{
		return this.getWrappedStorageManager().operationModeTime();
	}

	@Override
	public boolean isActive()
	{
		return this.getWrappedStorageManager().isActive();
	}

	@Override
	public boolean issueGarbageCollection(final long nanoTimeBudget)
	{
		return this.getWrappedStorageManager().issueGarbageCollection(nanoTimeBudget);
	}

	@Override
	public boolean issueFileCheck(final long nanoTimeBudget)
	{
		return this.getWrappedStorageManager().issueFileCheck(nanoTimeBudget);
	}

	@Override
	public boolean issueCacheCheck(final long nanoTimeBudget, final StorageEntityCacheEvaluator entityEvaluator)
	{
		return this.getWrappedStorageManager().issueCacheCheck(nanoTimeBudget, entityEvaluator);
	}

	@Override
	public void issueFullBackup(
		final StorageLiveFileProvider targetFileProvider,
		final PersistenceTypeDictionaryExporter typeDictionaryExporter
	)
	{
		this.getWrappedStorageManager().issueFullBackup(targetFileProvider, typeDictionaryExporter);
	}

	@Override
	public void issueTransactionsLogCleanup()
	{
		this.getWrappedStorageManager().issueTransactionsLogCleanup();
	}

	@Override
	public StorageRawFileStatistics createStorageStatistics()
	{
		return this.getWrappedStorageManager().createStorageStatistics();
	}

	@Override
	public void exportChannels(final StorageLiveFileProvider fileProvider, final boolean performGarbageCollection)
	{
		this.getWrappedStorageManager().exportChannels(fileProvider, performGarbageCollection);
	}

	@Override
	public StorageEntityTypeExportStatistics exportTypes(
		final StorageEntityTypeExportFileProvider exportFileProvider,
		final Predicate<? super StorageEntityTypeHandler> isExportType
	)
	{
		return this.getWrappedStorageManager().exportTypes(exportFileProvider, isExportType);
	}

	@Override
	public void importFiles(final XGettingEnum<AFile> importFiles)
	{
		this.getWrappedStorageManager().importFiles(importFiles);
	}

	@Override
	public void importData(final XGettingEnum<ByteBuffer> importData)
	{
		this.getWrappedStorageManager().importData(importData);
	}

	@Override
	public PersistenceManager<Binary> persistenceManager()
	{
		return this.getWrappedStorageManager().persistenceManager();
	}
}
