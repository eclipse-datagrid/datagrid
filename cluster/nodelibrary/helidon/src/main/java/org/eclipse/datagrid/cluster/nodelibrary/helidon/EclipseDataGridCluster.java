package org.eclipse.datagrid.cluster.nodelibrary.helidon;

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterFoundation;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageManager;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.serializer.concurrency.LockedExecutor;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Helidon
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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class EclipseDataGridCluster
{
	@ApplicationScoped
	@Produces
	public ObjectGraphUpdateHandler objectGraphUpdateHandler(final LockedExecutor executor)
	{
		return updater -> executor.write(updater::updateObjectGraph);
	}

	@ApplicationScoped
	@Produces
	public LockedExecutor lockedExecutor()
	{
		return LockedExecutor.New();
	}

	@SuppressWarnings("rawtypes")
	@ApplicationScoped
	@Produces
	public ClusterStorageManager clusterStorageManager(final ClusterFoundation foundation)
	{
		final var manager = foundation.startStorageManager();
		Runtime.getRuntime().addShutdownHook(new Thread(manager::close, "ShutdownCluster"));
		return manager;
	}

	@SuppressWarnings("rawtypes")
	@ApplicationScoped
	@Produces
	public ClusterRestRequestController clusterRequestController(final ClusterFoundation foundation)
	{
		final var controller = foundation.startController();
		Runtime.getRuntime().addShutdownHook(new Thread(controller::close, "ShutdownController"));
		return controller;
	}

	@SuppressWarnings("rawtypes")
	@ApplicationScoped
	@Produces
	public ClusterFoundation clusterFoundation(
		final RootProvider rootProvider,
		final ObjectGraphUpdateHandler objectGraphUpdateHandler,
		@ConfigProperty(name = "eclipsestore.distribution.kafka.async", defaultValue = "false") final boolean async
	)
	{
		return ClusterFoundation.New()
			.setEnableAsyncDistribution(async)
			.setObjectGraphUpdateHandler(objectGraphUpdateHandler)
			.setRootSupplier(rootProvider::root);
	}
}
