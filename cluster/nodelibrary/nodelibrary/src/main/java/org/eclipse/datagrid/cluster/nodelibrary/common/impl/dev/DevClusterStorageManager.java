package org.eclipse.datagrid.cluster.nodelibrary.common.impl.dev;

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
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageManager;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;

import java.util.function.Supplier;


public class DevClusterStorageManager<T> extends ClusterStorageManager.Abstract<T>
{
	private final EmbeddedStorageManager storage;

	public DevClusterStorageManager(final Supplier<T> rootSupplier)
	{
		this(rootSupplier, EmbeddedStorageConfiguration.Builder());
	}

	public DevClusterStorageManager(final Supplier<T> rootSupplier, final EmbeddedStorageConfigurationBuilder config)
	{
		this.storage = config.createEmbeddedStorageFoundation().start();
		if (this.storage.root() == null)
		{
			final var root = rootSupplier.get();
			if (root instanceof Lazy)
			{
				this.storage.setRoot(root);
			}
			else
			{
				this.storage.setRoot(Lazy.Reference(root));
			}
			this.storage.storeRoot();
		}
	}

	@Override
	public void activateDistribution()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isReady()
	{
		return true;
	}

	@Override
	public boolean isDistributor()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public long getCurrentOffset()
	{
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Lazy<T> root()
	{
		return (Lazy<T>)this.storage.root();
	}

	@Override
	protected StorageManager delegate()
	{
		return this.storage;
	}
	
	@Override
	public long store(final Object instance)
	{
		return this.storage.store(instance);
	}
	
	@Override
	public void storeAll(final Iterable<?> instances)
	{
		this.storage.storeAll(instances);
	}
	
	@Override
	public long[] storeAll(final Object... instances)
	{
		return this.storage.storeAll(instances);
	}
	
	@Override
	public long storeRoot()
	{
		return this.storage.storeRoot();
	}
	
	@Override
	public Storer createEagerStorer()
	{
		return this.storage.createEagerStorer();
	}
	
	@Override
	public Storer createLazyStorer()
	{
		return this.storage.createLazyStorer();
	}
	
	@Override
	public Storer createStorer()
	{
		return this.storage.createStorer();
	}
}
