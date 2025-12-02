package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterFoundation;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class ClusterRestRequestControllerFactory
{
	@Singleton
	@Bean(preDestroy = "close")
	public ClusterRestRequestController clusterRequestController(final ClusterFoundation<?> foundation)
	{
		return foundation.startController();
	}
}
