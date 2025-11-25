package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Micronaut
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
    @Singleton
    @Bean(preDestroy = "close")
    @Replaces(StorageManager.class)
    public ClusterStorageManager<?> clusterStorageManager(final ClusterFoundation<?> foundation)
    {
        return foundation.startStorageManager();
    }
}
