package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

import java.util.function.Supplier;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.exceptions.DisabledBeanException;
import io.micronaut.core.reflect.InstantiationUtils;
import io.micronaut.eclipsestore.conf.EmbeddedStorageConfigurationProvider;
import io.micronaut.eclipsestore.conf.RootClassConfigurationProvider;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterFoundation;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;

@Factory
public class ClusterFoundationFactory
{
	private final BeanContext context;

	public ClusterFoundationFactory(final BeanContext context)
	{
		this.context = context;
	}

	@Singleton
	public ClusterFoundation<?> clusterFoundation(
		final EmbeddedStorageConfigurationProvider configProvider,
		@Property(name = "eclipsestore.distribution.kafka.async", defaultValue = "false") final boolean async,
		final ObjectGraphUpdateHandler objectGraphUpdateHandler
	)
	{
		final String name = "main";

		if (!this.context.containsBean(RootClassConfigurationProvider.class, Qualifiers.byName(name)))
		{
			throw new DisabledBeanException(
				"Please, define a bean of type " + RootClassConfigurationProvider.class.getSimpleName()
					+ " by name qualifier: " + name
			);
		}

		final RootClassConfigurationProvider configuration = this.context.getBean(
			RootClassConfigurationProvider.class,
			Qualifiers.byName(name)
		);

		if (configuration.getRootClass() == null)
		{
			throw new RuntimeException(
				"No EclipseStore storage root was found in the configuration for storage with name " + name
			);
		}

		final Supplier<Object> rootSupplier = () -> InstantiationUtils.instantiate(configuration.getRootClass());

		return ClusterFoundation.New()
			.setEnableAsyncDistribution(async)
			.setObjectGraphUpdateHandler(objectGraphUpdateHandler)
			.setRootSupplier(rootSupplier)
			.setEmbeddedStorageFoundation(configProvider.getBuilder().createEmbeddedStorageFoundation());
	}
}
