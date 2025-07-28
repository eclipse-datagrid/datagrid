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

import org.eclipse.serializer.persistence.types.Storer;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.embedded.configuration.types.EmbeddedStorageConfiguration;
import org.eclipse.store.storage.embedded.configuration.types.EmbeddedStorageConfigurationBuilder;
import org.eclipse.store.storage.types.StorageManager;

import org.eclipse.datagrid.cluster.nodelibrary.common.backup.BackupStorage;

import java.util.function.Supplier;


public class BackupDefaultClusterStorageManager<T> extends DefaultClusterStorageManager.Abstract<T>
{
	private static final String NO_STORE_CALLS = "Backup Nodes don't support store calls";

	public BackupDefaultClusterStorageManager(final Supplier<T> rootSupplier)
	{
		this(rootSupplier, EmbeddedStorageConfiguration.Builder());
	}
	
	public BackupDefaultClusterStorageManager(
		final Supplier<T> rootSupplier,
		final EmbeddedStorageConfigurationBuilder config
	)
	{
		BackupStorage.setRootSupplier(rootSupplier);
		BackupStorage.setBackupStorageManagerConfig(config);
		BackupStorage.get();
	}

	@Override
	public void issueFullFileCheck()
	{
		BackupStorage.get().issueFullFileCheck();
	}

	@Override
	public boolean issueGarbageCollection(final long nanoTimeBudget)
	{
		return BackupStorage.get().issueGarbageCollection(nanoTimeBudget);
	}

	@Override
	public long getCurrentOffset()
	{
		throw new UnsupportedOperationException("Only Storage Nodes can return their current offset");
	}

	@Override
	public Lazy<T> root()
	{
		return null;
	}
	
	@Override
	public void startDistributionActivation()
	{
		throw new UnsupportedOperationException("Backup Nodes don't support distribution");
	}
	
	@Override
	public boolean finishDistributionActivation()
	{
		throw new UnsupportedOperationException("Backup Nodes don't support distribution");
	}

	@Override
	public boolean isDistributor()
	{
		throw new UnsupportedOperationException("Backup Nodes don't support distribution");
	}

	@Override
	public long store(final Object instance)
	{
		throw new UnsupportedOperationException(NO_STORE_CALLS);
	}

	@Override
	public void storeAll(final Iterable<?> instances)
	{
		throw new UnsupportedOperationException(NO_STORE_CALLS);
	}

	@Override
	public long[] storeAll(final Object... instances)
	{
		throw new UnsupportedOperationException(NO_STORE_CALLS);
	}

	@Override
	public long storeRoot()
	{
		throw new UnsupportedOperationException(NO_STORE_CALLS);
	}

	@Override
	public Storer createEagerStorer()
	{
		throw new UnsupportedOperationException(NO_STORE_CALLS);
	}

	@Override
	public Storer createLazyStorer()
	{
		throw new UnsupportedOperationException(NO_STORE_CALLS);
	}

	@Override
	public Storer createStorer()
	{
		throw new UnsupportedOperationException(NO_STORE_CALLS);
	}
	
	@Override
	public boolean shutdown()
	{
		if (BackupStorage.isRunning())
		{
			BackupStorage.get().close();
		}
		return true;
	}

	@Override
	protected StorageManager delegate()
	{
		throw new UnsupportedOperationException("Backup Nodes don't support direct storage manipulation");
	}
}
