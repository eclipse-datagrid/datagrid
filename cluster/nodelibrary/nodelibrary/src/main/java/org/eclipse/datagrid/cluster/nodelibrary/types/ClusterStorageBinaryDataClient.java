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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAcceptor;
import org.eclipse.serializer.collections.EqHashTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.eclipse.serializer.util.X.notNull;


public interface ClusterStorageBinaryDataClient extends StorageBinaryDataClient
{
    void stopAtLatestOffset();

    OffsetInfo offsetInfo();

    boolean isRunning();

    static ClusterStorageBinaryDataClient New(
        final StorageBinaryDataPacketAcceptor packetAcceptor,
        final String topicName,
        final String groupId,
        final AfterDataMessageConsumedListener offsetChangedListener,
        final OffsetInfo startingOffsetInfo,
        final KafkaPropertiesProvider kafkaPropertiesProvider,
        final boolean doCommitOffset
    )
    {
        return new Default(
            notNull(packetAcceptor),
            notNull(topicName),
            notNull(groupId),
            notNull(offsetChangedListener),
            notNull(startingOffsetInfo),
            notNull(kafkaPropertiesProvider),
            doCommitOffset
        );
    }

    final class Default implements ClusterStorageBinaryDataClient
    {
        private static final Logger LOG = LoggerFactory.getLogger(ClusterStorageBinaryDataClient.class);
        private static final long PARTITION_ASSIGNMENT_TIMEOUT_MS = Duration.ofSeconds(60L).toMillis();
        private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5L);

        /**
         * List of packets that have been polled but not yet consumed as they are still
         * missing some packets to complete the set
         */
        private final Queue<ClusterStorageBinaryDataPacket> cachedPackets = new LinkedList<>();

        private final StorageBinaryDataPacketAcceptor packetAcceptor;
        private final String topicName;
        private final String groupId;
        private final AfterDataMessageConsumedListener offsetChangedListener;
        private final KafkaPropertiesProvider kafkaPropertiesProvider;
        private final boolean doCommitOffset;

        private final AtomicReference<OffsetInfo> offsetInfo;
        private long cachedOffset;
        private final AtomicBoolean active = new AtomicBoolean();
        private final AtomicBoolean stopAtLatestOffset = new AtomicBoolean();

        public Default(
            final StorageBinaryDataPacketAcceptor packetAcceptor,
            final String topicName,
            final String groupId,
            final AfterDataMessageConsumedListener offsetChangedListener,
            final OffsetInfo startingOffsetInfo,
            final KafkaPropertiesProvider kafkaPropertiesProvider,
            final boolean doCommitOffset
        )
        {
            this.packetAcceptor = packetAcceptor;
            this.topicName = topicName;
            this.groupId = groupId;
            this.offsetChangedListener = offsetChangedListener;
            this.offsetInfo = new AtomicReference<>(startingOffsetInfo);
            this.cachedOffset = startingOffsetInfo.msOffset();
            this.kafkaPropertiesProvider = kafkaPropertiesProvider;
            this.doCommitOffset = doCommitOffset;
        }

        @Override
        public boolean isRunning()
        {
            return this.active.get();
        }

        @Override
        public OffsetInfo offsetInfo()
        {
            return this.offsetInfo.get();
        }

        @Override
        public void start()
        {
            this.active.set(true);
            if (LOG.isInfoEnabled())
            {
                LOG.info("Starting kafka data client at offsets {}", this.offsetInfo.get().msOffset());
            }
            final Thread thread = new Thread(this::tryRun);
            thread.start();
        }

        private void tryRun()
        {
            try
            {
                this.run();
            }
            catch (final Throwable t)
            {
                GlobalErrorHandling.handleFatalError(t);
            }
        }

        private void run()
        {
            final Properties properties = this.kafkaPropertiesProvider.provide();
            properties.setProperty(GROUP_ID_CONFIG, this.groupId);
            properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
            properties.setProperty(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

            try (final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties))
            {
                consumer.subscribe(Collections.singletonList(this.topicName));

                // Wait for assignment
                LOG.trace("Waiting for partition assignment");
                final long startMs = System.currentTimeMillis();
                final long endMs = startMs + PARTITION_ASSIGNMENT_TIMEOUT_MS;
                while (consumer.assignment().isEmpty())
                {
                    if (System.currentTimeMillis() > endMs)
                    {
                        throw new RuntimeException("Timed out waiting for topic partition assignment");
                    }
                    consumer.poll(POLL_TIMEOUT);
                }

                // Seek to correct offsets
                final var cachedOffsetInfo = this.offsetInfo.get();
                for (final var entry : cachedOffsetInfo.kafkaPartitionOffsets())
                {
                    final var partition = entry.key();
                    final long offset = entry.value();
                    LOG.debug("Seeking partition {} to offset {}", partition, offset);
                    consumer.seek(partition, offset);
                }
                final var missingPartitions = consumer.assignment()
                    .stream()
                    .filter(a -> !cachedOffsetInfo.kafkaPartitionOffsets().containsSearched(kv -> kv.key().equals(a)))
                    .toList();
                if (!missingPartitions.isEmpty())
                {
                    LOG.debug(
                        "Resetting offsets for the following partitions missing in the starting offsets: {}",
                        missingPartitions
                    );
                    consumer.seekToBeginning(missingPartitions);
                }

                while (this.active.get())
                {
                    if (!this.stopAtLatestOffset.get())
                    {
                        this.pollAndConsume(consumer);
                    }
                    else
                    {
                        LOG.info("Data client is now stopping at latest offset.");
                        final long stopAt;
                        try (
                            final var offsetProvider = KafkaOffsetProvider.forTopic(
                                this.topicName,
                                this.kafkaPropertiesProvider
                            )
                        )
                        {
                            offsetProvider.init();
                            stopAt = offsetProvider.provideLatestOffset();
                        }
                        LOG.info("Stopping at offset {}", stopAt);

                        while (this.cachedOffset < stopAt)
                        {
                            this.pollAndConsume(consumer);
                        }
                        LOG.info("Data client is now at latest offset ({})", this.cachedOffset);

                        this.active.set(false);
                    }

                    if (this.doCommitOffset)
                    {
                        consumer.commitSync();
                    }
                }
            }

            LOG.info("DataClient run finished");
        }

        private OffsetInfo updateOffsets(final KafkaConsumer<String, byte[]> consumer)
        {
            if (LOG.isDebugEnabled() && this.cachedOffset % 10_000 == 0)
            {
                LOG.debug("Polling and updating offset info for offset {}", this.cachedOffset);
            }
            final EqHashTable<TopicPartition, Long> map = EqHashTable.New();
            for (final var partition : consumer.assignment())
            {
                final var offset = consumer.position(partition);
                map.put(partition, offset);
            }
            final var info = OffsetInfo.New(this.cachedOffset, map.immure());
            this.offsetInfo.set(info);
            return info;
        }

        /**
         * Polls the kafka consumer and consumes fully completed messages. Invalid
         * packets are skipped and incomplete messages will be cached.
         */
        private void pollAndConsume(final KafkaConsumer<String, byte[]> consumer)
        {
            // only consume complete messages, to do this we need to look ahead to see if all packets
            // are here yet. If not read more, if it starts at 0 again then we know that something went
            // wrong on the writer side and that we should just skip all the packets in that series

            this.cachedPackets.addAll(this.createPackets(consumer.poll(Duration.ofSeconds(5))));

            if (this.cachedPackets.isEmpty())
            {
                return;
            }

            outer: while (!this.cachedPackets.isEmpty())
            {
                final var rootPacket = this.cachedPackets.peek();

                if (rootPacket.packetIndex() != 0)
                {
                    LOG.error("First packet has index {}, expected 0 skipping packet...", rootPacket.packetIndex());
                    this.cachedPackets.remove();
                    continue;
                }

                if (this.cachedPackets.size() < rootPacket.packetCount())
                {
                    //LOG.trace("Message Incomplete ({}/{})", this.cachedPackets.size(), rootPacket.packetCount());
                    return;
                }

                final var packets = new ArrayList<ClusterStorageBinaryDataPacket>();

                for (int i = 0; i < rootPacket.packetCount(); i++)
                {
                    final var packet = this.cachedPackets.remove();

                    if (packet.packetIndex() != i)
                    {
                        LOG.error(
                            "Unexpected Packet Index {}, expected 0 skipping packet...",
                            rootPacket.packetIndex()
                        );
                        continue outer;
                    }

                    if (packet.packetCount() != rootPacket.packetCount())
                    {
                        LOG.error(
                            "Unexpected Packet Count {} of Packet at Index {}, expected {} skipping packet...",
                            packet.packetCount(),
                            packet.packetIndex(),
                            rootPacket.packetCount()
                        );
                        continue outer;
                    }

                    packets.add(packet);
                }

                this.consumeFullMessage(packets, consumer);
            }
        }

        private List<ClusterStorageBinaryDataPacket> createPackets(final ConsumerRecords<String, byte[]> records)
        {
            final var list = new ArrayList<ClusterStorageBinaryDataPacket>();
            for (final var record : records)
            {
                if (record.serializedValueSize() > 0)
                {
                    list.add(this.createDataPacket(record, record.headers()));
                }
                else
                {
                    LOG.warn("Encountered record with serialized value size 0");
                }
            }
            return list;
        }

        private void consumeFullMessage(
            final Iterable<ClusterStorageBinaryDataPacket> packets,
            final KafkaConsumer<String, byte[]> consumer
        )
        {
            final List<StorageBinaryDataPacket> newPackets = new ArrayList<>();

            for (final var packet : packets)
            {
                if (this.cachedOffset >= packet.microstreamOffset())

                {
                    LOG.warn(
                        "Skipping packet with offset {} (current: {})",
                        packet.microstreamOffset(),
                        this.cachedOffset
                    );
                    continue;
                }


                this.cachedOffset = packet.microstreamOffset();



                if (LOG.isTraceEnabled() && this.cachedOffset % 10_000 == 0)
                {
                    LOG.trace("Consuming packet with offset {}", this.cachedOffset);
                }

                newPackets.add(packet);



            }

            if (!newPackets.isEmpty())
            {
                if (LOG.isDebugEnabled() && this.cachedOffset % 10_000 == 0)
                {
                    LOG.debug("Applying packets at offset {}", this.cachedOffset);
                }
                this.packetAcceptor.accept(newPackets);
                final var newInfo = this.updateOffsets(consumer);
                this.offsetChangedListener.onChange(newInfo);
            }
        }

        private ClusterStorageBinaryDataPacket createDataPacket(
            final ConsumerRecord<String, byte[]> record,
            final Headers headers
        )
        {
            return ClusterStorageBinaryDataPacket.New(
                ClusterStorageBinaryDistributedKafka.messageType(headers),
                ClusterStorageBinaryDistributedKafka.messageLength(headers),
                ClusterStorageBinaryDistributedKafka.packetIndex(headers),
                ClusterStorageBinaryDistributedKafka.packetCount(headers),
                ClusterStorageBinaryDistributedKafka.microstreamOffset(headers),
                ByteBuffer.wrap(record.value())
            );
        }

        /**
         * Stop collecting updates after the last available offset in kafka has been
         * reached. After the offset has been reached a file called 'stopped' will be
         * created in the storage directory.
         */
        @Override
        public void stopAtLatestOffset()
        {
            LOG.info("DataClient will stop at latest offset");
            this.stopAtLatestOffset.set(true);
        }

        @Override
        public void dispose()
        {
            LOG.trace("Disposing data client");
            this.active.set(false);
            this.offsetChangedListener.close();
        }
    }
}
