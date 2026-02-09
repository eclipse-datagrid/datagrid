package org.eclipse.datagrid.cache.clustered.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 * 
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.hibernate.cache.internal.BasicCacheKeyImplementation;
import org.hibernate.cache.internal.CacheKeyImplementation;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface SerializationTypesProvider
{
    Collection<Class<?>> provideTypes();

    class Default implements SerializationTypesProvider
    {
        @Override
        public Collection<Class<?>> provideTypes()
        {
            return List.of(
                // Hibernate cache key types
                CacheKeyImplementation.class,
                BasicCacheKeyImplementation.class,
                // message types
                ClusteredCacheMessage.class,
                CacheInvalidationMessage.class,
                TimestampsRegionUpdateMessage.class,
                // common cache key id types
                UUID.class
            );
        }
    }
}
