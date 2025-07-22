package org.eclipse.datagrid.cluster.nodelibrary.micronaut;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary Micronaut
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

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.*;
import io.micronaut.context.exceptions.DisabledBeanException;
import io.micronaut.core.reflect.InstantiationUtils;
import io.micronaut.eclipsestore.conf.EmbeddedStorageConfigurationProvider;
import io.micronaut.eclipsestore.conf.RootClassConfigurationProvider;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl._default.BackupDefaultClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl._default.NodeDefaultClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl.dev.DevClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl.micro.MicroClusterStorageManager;
import org.eclipse.store.storage.types.StorageManager;

import java.util.function.Supplier;


// Guided by https://github.com/micronaut-projects/micronaut-eclipsestore/blob/1.9.x/eclipsestore/src/main/java/io/micronaut/eclipsestore/conf/StorageManagerFactory.java
@Factory
public class ClusterStorageManagerFactory
{
	private final BeanContext context;

	public ClusterStorageManagerFactory(final BeanContext context)
	{
		this.context = context;
	}
	
	@Singleton
	@Bean(preDestroy = "close")
	@Replaces(StorageManager.class)
	public ClusterStorageManager<?> clusterStorageManager(
		final EmbeddedStorageConfigurationProvider configProvider,
		@Property(name = "eclipse.datagrid.distribution.kafka.async", defaultValue = "false") final boolean async
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
		final Supplier<?> rootSupplier = () -> InstantiationUtils.instantiate(configuration.getRootClass());
		final var         builder      = configProvider.getBuilder();

		final ClusterStorageManager<?> sm;
		
		if (!ClusterEnv.isProdMode())
		{
			sm = new DevClusterStorageManager<>(rootSupplier, builder);
		}
		else if (ClusterEnv.isMicro())
		{
			sm = new MicroClusterStorageManager<>(rootSupplier, builder);
		}
		else if (ClusterEnv.isBackupNode())
		{
			sm = new BackupDefaultClusterStorageManager<>(rootSupplier, builder);
		}
		else
		{
			sm = new NodeDefaultClusterStorageManager<>(rootSupplier, builder, async);
		}

		// NOTE: For some reason @Bean(preDestroy = "close") does nothing, so we add our own shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(sm::close, "ShutdownCluster"));

		return sm;
	}
}
