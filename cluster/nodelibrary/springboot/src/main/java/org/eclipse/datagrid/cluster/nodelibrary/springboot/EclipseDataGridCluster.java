package org.eclipse.datagrid.cluster.nodelibrary.springboot;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary Spring Boot
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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl._default.BackupDefaultClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl._default.NodeDefaultClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl.dev.DevClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl.micro.MicroClusterStorageManager;

@Configuration
@Import(StorageClusterController.class)
public class EclipseDataGridCluster
{
	@Bean
	public ClusterStorageManager<?> clusterStorageManager(
		final RootProvider<?> rootProvider,
		@Value("${eclipse.datagrid.distribution.kafka.async:false}") final boolean async
	)
	{
		final var root = rootProvider.root();

		if (!ClusterEnv.isProdMode())
		{
			return new DevClusterStorageManager<>(rootProvider::root);
		}

		if (ClusterEnv.isMicro())
		{
			return new MicroClusterStorageManager<>(rootProvider::root);
		}

		if (ClusterEnv.isBackupNode())
		{
			return new BackupDefaultClusterStorageManager<>(rootProvider::root);
		}

		return new NodeDefaultClusterStorageManager<>(rootProvider::root, async);
	}
}
