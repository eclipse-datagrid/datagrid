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

import org.eclipse.store.cache.types.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusteredCacheMessageAcceptor
{
    private static final Logger logger = LoggerFactory.getLogger(ClusteredCacheMessageAcceptor.class);
    private final CacheManager cacheManager;

    public ClusteredCacheMessageAcceptor(final CacheManager cacheManager)
    {
        this.cacheManager = cacheManager;
    }

    public void accept(final TimestampsRegionUpdateMessage message)
    {
        final var cache = this.cacheManager.getCache(message.cacheName());

        if (cache == null)
        {
            // we don't have this cache loaded
            return;
        }

        final Long previousTimestamp = (Long)cache.get(message.tableName());
        if (previousTimestamp != null && previousTimestamp > message.timestamp())
        {
            // we received an outdated message
            logger.debug(
                "Received outdated query-cache timestamp table={}, timestamp={}. Currently stored timestamp={}",
                message.tableName(),
                message.timestamp(),
                previousTimestamp
            );
            return;
        }

        logger.debug(
            "Updating query-cache timestamp table={}, timestamp={}.",
            message.tableName(),
            message.timestamp()
        );

        cache.putSilent(message.tableName(), message.timestamp());
    }
}
