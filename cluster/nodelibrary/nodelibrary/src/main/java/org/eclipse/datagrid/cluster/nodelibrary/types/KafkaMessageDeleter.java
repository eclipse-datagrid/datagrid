package org.eclipse.datagrid.cluster.nodelibrary.types;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.collections.types.XImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface KafkaMessageDeleter
{
    static KafkaMessageDeleter New(final KafkaAdminClient kafkaAdminClient)
    {
        return new Default(notNull(kafkaAdminClient));
    }

    void deleteUntilOffsets(XImmutableMap<TopicPartition, Long> partitionOffsets) throws NodelibraryException;

    class Default implements KafkaMessageDeleter
    {
        private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageDeleter.class);

        private final KafkaAdminClient kafkaAdminClient;

        private Default(final KafkaAdminClient kafkaAdminClient)
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
