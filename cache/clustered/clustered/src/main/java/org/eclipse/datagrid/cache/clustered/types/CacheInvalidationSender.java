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

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;

public class CacheInvalidationSender<K, V> implements CacheEntryUpdatedListener<K, V>
{
    @Override
    public void onUpdated(final Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
        throws CacheEntryListenerException
    {
        for (final var event : cacheEntryEvents)
        {
            final var cacheName = event.getSource().getName();
            final var key = event.getKey();
            System.out.printf("Entry Updated { Cache='%s', Key='%s' }%n", cacheName, key);
        }
    }
}
