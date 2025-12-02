package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterFoundation;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageManager;
import org.eclipse.store.storage.types.StorageManager;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;

@Factory
public class ClusterStorageManagerFactory
{
	@Replaces(StorageManager.class)
	@Bean(preDestroy = "shutdown")
	@Singleton
	public ClusterStorageManager<?> clusterStorageManager(final ClusterFoundation<?> foundation)
	{
		return foundation.startStorageManager();
	}
}
