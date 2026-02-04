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

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;

public class ClusteredCacheEntryListenerConfiguration<K, V> implements CacheEntryListenerConfiguration<K, V>
{
    private final CacheInvalidationSender<K, V> cacheInvalidationSender;

    public ClusteredCacheEntryListenerConfiguration(final CacheInvalidationSender<K, V> cacheInvalidationSender)
    {
        this.cacheInvalidationSender = cacheInvalidationSender;
    }

    @Override
    public Factory<CacheEntryListener<? super K, ? super V>> getCacheEntryListenerFactory()
    {
        return () -> cacheInvalidationSender;
    }

    @Override
    public boolean isOldValueRequired()
    {
        return false;
    }

    @Override
    public Factory<CacheEntryEventFilter<? super K, ? super V>> getCacheEntryEventFilterFactory()
    {
        return null;
    }

    @Override
    public boolean isSynchronous()
    {
        return true;
    }
}
