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

import org.eclipse.store.cache.hibernate.types.CacheRegionFactory;
import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.support.DomainDataStorageAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;

import javax.cache.configuration.CacheEntryListenerConfiguration;

public class ClusteredCacheRegionFactory extends CacheRegionFactory
{
    private final CacheEntryListenerConfiguration<Object, Object> cacheEntryListenerConfiguration =
        new ClusteredCacheEntryListenerConfiguration<>(new CacheInvalidationSender<>());

    public ClusteredCacheRegionFactory()
    {
        super();
    }

    public ClusteredCacheRegionFactory(final CacheKeysFactory cacheKeysFactory)
    {
        super(cacheKeysFactory);
    }

    @Override
    protected StorageAccess createTimestampsRegionStorageAccess(
        final String regionName,
        final SessionFactoryImplementor sessionFactory
    )
    {
        final String defaultedRegionName = this.defaultRegionName(
            regionName,
            sessionFactory,
            DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME,
            LEGACY_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAMES
        );
        final var cache = this.getOrCreateCache(defaultedRegionName, sessionFactory);
        cache.registerCacheEntryListener(this.cacheEntryListenerConfiguration);
        return StorageAccess.New(cache);
    }

    @Override
    protected DomainDataStorageAccess createDomainDataStorageAccess(
        final DomainDataRegionConfig regionConfig,
        final DomainDataRegionBuildingContext buildingContext
    )
    {
        final var cache = this.getOrCreateCache(regionConfig.getRegionName(), buildingContext.getSessionFactory());
        cache.registerCacheEntryListener(this.cacheEntryListenerConfiguration);
        return StorageAccess.New(cache);
    }
}
