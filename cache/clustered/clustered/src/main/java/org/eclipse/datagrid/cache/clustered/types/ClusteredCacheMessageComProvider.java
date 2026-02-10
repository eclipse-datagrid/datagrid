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

import java.util.Map;

import org.eclipse.serializer.Serializer;

public interface ClusteredCacheMessageComProvider<K, V>
{
    ClusteredCacheMessageSender<K, V> provideUpdateTimestampsCacheMessageSender(
        @SuppressWarnings("rawtypes") Map properties,
        Serializer<byte[]> serializer
    );

    ClusteredCacheMessageSender<K, V> provideUpdateCacheInvalidationMessageSender(
        @SuppressWarnings("rawtypes") Map properties,
        Serializer<byte[]> serializer
    );

    ClusteredCacheMessageReceiver provideMessageReceiver(
        @SuppressWarnings("rawtypes") Map properties,
        Serializer<byte[]> serializer,
        ClusteredCacheMessageAcceptor messageAcceptor
    );
}
