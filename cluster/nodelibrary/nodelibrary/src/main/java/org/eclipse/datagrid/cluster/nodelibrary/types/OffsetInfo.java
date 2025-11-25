package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

import static org.eclipse.serializer.util.X.notNull;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.serializer.collections.types.XImmutableMap;

public interface OffsetInfo
{
    long msOffset();

    XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets();

    static OffsetInfo New(final long msOffset, final XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets)
    {
        return new Default(msOffset, notNull(kafkaPartitionOffsets));
    }

    final class Default implements OffsetInfo
    {
        private final long msOffset;
        private final XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets;

        private Default(final long msOffset, final XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets)
        {
            this.msOffset = msOffset;
            this.kafkaPartitionOffsets = kafkaPartitionOffsets;
        }

        @Override
        public XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets()
        {
            return this.kafkaPartitionOffsets;
        }

        @Override
        public long msOffset()
        {
            return this.msOffset;
        }
    }
}
