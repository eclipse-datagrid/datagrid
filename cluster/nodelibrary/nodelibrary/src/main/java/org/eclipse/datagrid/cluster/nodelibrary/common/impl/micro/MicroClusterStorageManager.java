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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.embedded.configuration.types.EmbeddedStorageConfiguration;
import org.eclipse.store.storage.embedded.configuration.types.EmbeddedStorageConfigurationBuilder;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageManager;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.StorageLimitChecker;

public class MicroClusterStorageManager<T> extends ClusterStorageManager.Abstract<T>
{
	private final Logger logger = LoggerFactory.getLogger(MicroClusterStorageManager.class);
	private final EmbeddedStorageManager storage;

	private boolean isReady = false;
	
	public MicroClusterStorageManager(final Supplier<T> rootSupplier)
	{
		this(rootSupplier, EmbeddedStorageConfiguration.Builder());
	}

	public MicroClusterStorageManager(final Supplier<T> rootSupplier, final EmbeddedStorageConfigurationBuilder config)
	{
		this.logger.info("Initializing EclipseStore (micro instance mode)");

		final var storagePath = Paths.get("/storage/storage");

		this.initStorageLimitChecker();
		this.storage = this.initStorage(rootSupplier, storagePath, config);

		this.isReady = true;
	}

	private EmbeddedStorageManager initStorage(
		final Supplier<T> rootSupplier,
		final Path storagePath,
		final EmbeddedStorageConfigurationBuilder config
	)
	{
		final EmbeddedStorageManager newStorage = config.setStorageDirectory(storagePath.toString())
			.createEmbeddedStorageFoundation()
			.start();

		if (newStorage.root() == null)
		{
			final var root = rootSupplier.get();
			if (root instanceof Lazy)
			{
				newStorage.setRoot(root);
			}
			else
			{
				newStorage.setRoot(Lazy.Reference(root));
			}

			newStorage.storeRoot();
		}

		return newStorage;
	}

	@Override
	public boolean shutdown()
	{
		this.logger.info("Disposing Cluster Resources");
		this.storage.close();
		return super.shutdown();
	}

	private void initStorageLimitChecker()
	{
		try
		{
			StorageLimitChecker.get().start();
		}
		catch (final Exception e)
		{
			throw new RuntimeException(e);
		}
		Runtime.getRuntime().addShutdownHook(new Thread(() ->
		{
			try
			{
				StorageLimitChecker.get().stop();
			}
			catch (final SchedulerException e)
			{
				this.logger.error("Failed to shutdown storage limit checker", e);
			}
		}));
	}
	
	@Override
	protected StorageManager delegate()
	{
		return this.storage;
	}

	@Override
	public void issueFullFileCheck()
	{
		throw new UnsupportedOperationException("Only backup nodes support this feature!");
	}

	@Override
	public void activateDistribution()
	{
		throw new UnsupportedOperationException("Unsupported logic for micro nodes");
	}

	@SuppressWarnings("unchecked")
	@Override
	public Lazy<T> root()
	{
		return (Lazy<T>)this.storage.root();
	}

	@Override
	public boolean isReady()
	{
		return this.isReady;
	}

	@Override
	public boolean isDistributor()
	{
		throw new UnsupportedOperationException("Unsupported logic for micro nodes");
	}

	@Override
	public long getCurrentOffset()
	{
		throw new UnsupportedOperationException("Unsupported logic for micro nodes");
	}
}
