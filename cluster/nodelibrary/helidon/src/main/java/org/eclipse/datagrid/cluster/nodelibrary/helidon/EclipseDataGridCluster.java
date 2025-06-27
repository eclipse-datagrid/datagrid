package org.eclipse.datagrid.cluster.nodelibrary.helidon;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary Helidon
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

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl._default.BackupDefaultClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl._default.NodeDefaultClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl.dev.DevClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl.micro.MicroClusterStorageManager;

@ApplicationScoped
public class EclipseDataGridCluster
{
	@SuppressWarnings("rawtypes")
	@ApplicationScoped
	@Produces
	public ClusterStorageManager clusterStorageManager(
		final RootProvider rootProvider,
		@ConfigProperty(name = "eclipse.datagrid.distribution.kafka.async", defaultValue = "false") final boolean async
	)
	{
		final ClusterStorageManager<?> sm;
		
		if (!ClusterEnv.isProdMode())
		{
			sm =  new DevClusterStorageManager<>(rootProvider::root);
		}
		else if (ClusterEnv.isMicro())
		{
			sm = new MicroClusterStorageManager<>(rootProvider::root);
		}
		else if (ClusterEnv.isBackupNode())
		{
			sm = new BackupDefaultClusterStorageManager<>(rootProvider::root);
		}
		else
		{
			sm = new NodeDefaultClusterStorageManager<>(rootProvider::root, async);
		}

		Runtime.getRuntime().addShutdownHook(new Thread(sm::close, "ShutdownCluster"));

		return sm;
	}
}
