package org.eclipse.datagrid.cluster.nodelibrary.spi;

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageManager;

public interface ClusterStorageManagerProvider
{
	ClusterStorageManager<?> provideClusterStorageManager();
}
