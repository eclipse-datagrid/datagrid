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

import org.apache.kafka.common.TopicPartition;
import org.eclipse.serializer.chars.VarString;
import org.eclipse.serializer.collections.types.XImmutableMap;

import static org.eclipse.serializer.util.X.notNull;

public interface MessageInfo
{
    long messageIndex();

    XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets();

    static MessageInfo New(final long messageIndex, final XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets)
    {
        return new Default(messageIndex, notNull(kafkaPartitionOffsets));
    }

    final class Default implements MessageInfo
    {
        private final long messageIndex;
        private final XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets;

        private Default(final long messageIndex, final XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets)
        {
            this.messageIndex = messageIndex;
            this.kafkaPartitionOffsets = kafkaPartitionOffsets;
        }

        @Override
        public XImmutableMap<TopicPartition, Long> kafkaPartitionOffsets()
        {
            return this.kafkaPartitionOffsets;
        }

        @Override
        public long messageIndex()
        {
            return this.messageIndex;
        }

        @Override
        public String toString()
        {
            return VarString.New()
                .add("MessageInfo{messageIndex=")
                .add(this.messageIndex)
                .add(",kafkaPartitionOffsets=")
                .add(this.kafkaPartitionOffsets.toString())
                .add('}')
                .toString();
        }
    }
}
