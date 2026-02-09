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

import org.eclipse.serializer.typing.Disposable;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;

public class ClusteredCacheEntryListenerConfiguration<K, V> implements Disposable
{
    private final CacheEntryListenerConfig updateTimestamps;
    private final CacheEntryListenerConfig cacheInvalidation;

    public ClusteredCacheEntryListenerConfiguration(
        final ClusteredCacheMessageSender<K, V> updateTimestampsSender,
        final ClusteredCacheMessageSender<K, V> cacheInvalidationSender
    )
    {
        this.updateTimestamps = new CacheEntryListenerConfig(updateTimestampsSender);
        this.cacheInvalidation = new CacheEntryListenerConfig(cacheInvalidationSender);
    }

    public CacheEntryListenerConfiguration<K, V> getUpdateTimestampsCacheEntryListenerConfiguration()
    {
        return this.updateTimestamps;
    }

    public CacheEntryListenerConfiguration<K, V> getCacheInvalidationCacheEntryListenerConfiguration()
    {
        return this.cacheInvalidation;
    }

    private boolean isOldValueRequired()
    {
        return false;
    }

    private Factory<CacheEntryEventFilter<? super K, ? super V>> getCacheEntryEventFilterFactory()
    {
        return null;
    }

    private boolean isSynchronous()
    {
        return true;
    }

    @Override
    public void dispose()
    {
        this.updateTimestamps.sender.dispose();
        this.cacheInvalidation.sender.dispose();
    }

    public class CacheEntryListenerConfig implements CacheEntryListenerConfiguration<K, V>
    {
        private final ClusteredCacheMessageSender<K, V> sender;

        private CacheEntryListenerConfig(final ClusteredCacheMessageSender<K, V> sender)
        {
            this.sender = sender;
        }

        @Override
        public Factory<CacheEntryListener<? super K, ? super V>> getCacheEntryListenerFactory()
        {
            return () -> this.sender;
        }

        @Override
        public boolean isOldValueRequired()
        {
            return ClusteredCacheEntryListenerConfiguration.this.isOldValueRequired();
        }

        @Override
        public Factory<CacheEntryEventFilter<? super K, ? super V>> getCacheEntryEventFilterFactory()
        {
            return ClusteredCacheEntryListenerConfiguration.this.getCacheEntryEventFilterFactory();
        }

        @Override
        public boolean isSynchronous()
        {
            return ClusteredCacheEntryListenerConfiguration.this.isSynchronous();
        }
    }
}
