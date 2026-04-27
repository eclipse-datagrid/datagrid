package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.collections.types.XImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface KafkaRecordDeleter
{
    static KafkaRecordDeleter New(final AdminClient kafkaAdminClient)
    {
        return new Default(notNull(kafkaAdminClient));
    }

    void deleteUntilOffsets(XImmutableMap<TopicPartition, Long> partitionOffsets) throws NodelibraryException;

    class Default implements KafkaRecordDeleter
    {
        private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordDeleter.class);

        private final AdminClient kafkaAdminClient;

        private Default(final AdminClient kafkaAdminClient)
        {
            this.kafkaAdminClient = kafkaAdminClient;
        }

        @Override
        public void deleteUntilOffsets(final XImmutableMap<TopicPartition, Long> partitionOffsets)
            throws NodelibraryException
        {
            LOG.trace("Deleting old unused Kafka records.");

            final var partitionRecordsToDelete = new HashMap<TopicPartition, RecordsToDelete>();
            partitionOffsets.forEach(entry ->
            {
                LOG.debug("Deleting to offset {} in partition {}", entry.value(), entry.key().partition());
                final var partition = entry.key();
                final var recordsToDelete = RecordsToDelete.beforeOffset(entry.value());
                partitionRecordsToDelete.put(partition, recordsToDelete);
            });
            final var deleteResult = this.kafkaAdminClient.deleteRecords(partitionRecordsToDelete);
            try
            {
                deleteResult.all().get(10, TimeUnit.MINUTES);
            }
            catch (final InterruptedException | ExecutionException e)
            {
                throw new NodelibraryException(e);
            }
            catch (final TimeoutException e)
            {
                LOG.warn("Timed out waiting for old records to be deleted.", e);
                throw new RuntimeException(e);
            }
        }
    }
}
