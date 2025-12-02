package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.eclipsestore.DefaultRootProvider;
import io.micronaut.eclipsestore.RootProvider;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageManager;

@EachBean(ClusterStorageManager.class)
@Replaces(DefaultRootProvider.class)
public class ClusterRootProvider<T> implements RootProvider<T>
{
	private final ClusterStorageManager<T> storageManager;

	public ClusterRootProvider(final ClusterStorageManager<T> storageManager)
	{
		this.storageManager = storageManager;
	}

	@Override
	public T root()
	{
		return this.storageManager.root().get();
	}
}
