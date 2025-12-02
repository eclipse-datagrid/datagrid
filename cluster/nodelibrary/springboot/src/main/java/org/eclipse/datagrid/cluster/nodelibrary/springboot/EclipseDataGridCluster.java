package org.eclipse.datagrid.cluster.nodelibrary.springboot;

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterFoundation;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageManager;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.serializer.concurrency.LockedExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(SpringBootClusterController.class)
public class EclipseDataGridCluster
{
	@Bean
	public ObjectGraphUpdateHandler objectGraphUpdateHandler(final LockedExecutor executor)
	{
		return updater -> executor.write(updater::updateObjectGraph);
	}

	@Bean
	public LockedExecutor lockedExecutor()
	{
		return LockedExecutor.New();
	}

	@Bean
	public ClusterFoundation<?> clusterFoundation(
		final RootProvider<?> rootProvider,
		final ObjectGraphUpdateHandler objectGraphUpdateHandler,
		@Value("${eclipsestore.distribution.kafka.async:false}") final boolean async
	)
	{
		return ClusterFoundation.New()
			.setEnableAsyncDistribution(async)
			.setObjectGraphUpdateHandler(objectGraphUpdateHandler)
			.setRootSupplier(rootProvider::root);
	}

	@Bean
	public ClusterRestRequestController nodelibraryClusterController(final ClusterFoundation<?> foundation)
	{
		return foundation.startController();
	}

	@Bean
	public ClusterStorageManager<?> clusterStorageManager(final ClusterFoundation<?> foundation)
	{
		return foundation.startStorageManager();
	}

}
