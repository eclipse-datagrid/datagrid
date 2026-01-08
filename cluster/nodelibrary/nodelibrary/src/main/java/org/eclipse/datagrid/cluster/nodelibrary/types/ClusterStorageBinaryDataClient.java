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
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
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
    void stopAtLatestMessage();

    MessageInfo messageInfo();

    boolean isRunning();

    void resume() throws NodelibraryException;

    static ClusterStorageBinaryDataClient New(
        final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
        final String topicName,
        final String groupId,
        final AfterDataMessageConsumedListener offsetChangedListener,
        final MessageInfo startingMessageInfo,
        final KafkaPropertiesProvider kafkaPropertiesProvider,
        final boolean doCommitOffset
    )
    {
        return new Default(
            notNull(packetAcceptor),
            notNull(topicName),
            notNull(groupId),
            notNull(offsetChangedListener),
            notNull(startingMessageInfo),
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
         * List of packets that have been polled but not yet consumed as they are still missing some packets to complete
         * the set
         */
        private final Queue<ClusterStorageBinaryDataPacket> cachedPackets = new LinkedList<>();

        private final ClusterStorageBinaryDataPacketAcceptor packetAcceptor;
        private final String topicName;
        private final String groupId;
        private final AfterDataMessageConsumedListener offsetChangedListener;
        private final KafkaPropertiesProvider kafkaPropertiesProvider;
        private final boolean doCommitOffset;

        private final AtomicReference<MessageInfo> messageInfo;
        private long cachedMessageIndex;
        private final AtomicBoolean stopAtLatestMessage = new AtomicBoolean();
        private final AtomicBoolean requestStop = new AtomicBoolean();
        private final AtomicBoolean running = new AtomicBoolean();

        private Thread runner;

        private Default(
            final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
            final String topicName,
            final String groupId,
            final AfterDataMessageConsumedListener offsetChangedListener,
            final MessageInfo startingMessageInfo,
            final KafkaPropertiesProvider kafkaPropertiesProvider,
            final boolean doCommitOffset
        )
        {
            this.packetAcceptor = packetAcceptor;
            this.topicName = topicName;
            this.groupId = groupId;
            this.offsetChangedListener = offsetChangedListener;
            this.messageInfo = new AtomicReference<>(startingMessageInfo);
            this.cachedMessageIndex = startingMessageInfo.messageIndex();
            this.kafkaPropertiesProvider = kafkaPropertiesProvider;
            this.doCommitOffset = doCommitOffset;
        }

        @Override
        public boolean isRunning()
        {
            return this.running.get();
        }

        @Override
        public MessageInfo messageInfo()
        {
            return this.messageInfo.get();
        }

        @Override
        public void start()
        {
            if (LOG.isInfoEnabled())
            {
                LOG.info("Starting kafka data client at message index {}", this.messageInfo.get().messageIndex());
            }
            this.runner = new Thread(this::tryRun);
            this.runner.start();
        }

        private void tryRun()
        {
            try
            {
                this.running.set(true);
                this.run();
                this.running.set(false);
            }
            catch (final Throwable t)
            {
                this.running.set(false);
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
                final var cachedMessageInfo = this.messageInfo.get();
                for (final var entry : cachedMessageInfo.kafkaPartitionOffsets())
                {
                    final var partition = entry.key();
                    final long offset = entry.value();
                    LOG.debug("Seeking partition {} to offset {}", partition, offset);
                    consumer.seek(partition, offset);
                }
                final var missingPartitions = consumer.assignment()
                    .stream()
                    .filter(a -> !cachedMessageInfo.kafkaPartitionOffsets().containsSearched(kv -> kv.key().equals(a)))
                    .toList();
                if (!missingPartitions.isEmpty())
                {
                    LOG.debug(
                        "Resetting offsets for the following partitions missing in the starting offsets: {}",
                        missingPartitions
                    );
                    consumer.seekToBeginning(missingPartitions);
                }

                boolean run = true;
                while (run && !this.requestStop.get())
                {
                    if (!this.stopAtLatestMessage.get())
                    {
                        this.pollAndConsume(consumer);
                    }
                    else
                    {
                        LOG.info("Data client is now stopping at latest message.");
                        final long stopAt;
                        try (
                            final var offsetProvider = KafkaMessageInfoProvider.New(
                                this.topicName,
                                this.groupId + "-offsetgetter",
                                this.kafkaPropertiesProvider
                            )
                        )
                        {
                            offsetProvider.init();
                            stopAt = offsetProvider.provideLatestMessageIndex();
                        }
                        LOG.info("Stopping at message index {}", stopAt);

                        while (this.cachedMessageIndex < stopAt && !this.requestStop.get())
                        {
                            this.pollAndConsume(consumer);
                        }
                        LOG.info("Data client is now at latest offset ({})", this.cachedMessageIndex);

                        this.stopAtLatestMessage.set(false);
                        run = false;
                    }

                    if (this.doCommitOffset)
                    {
                        consumer.commitSync();
                    }
                }
            }

            this.requestStop.set(false);
            LOG.info("DataClient run finished");
        }

        private MessageInfo updateOffsets(final KafkaConsumer<String, byte[]> consumer)
        {
            if (LOG.isDebugEnabled() && this.cachedMessageIndex % 10_000 == 0)
            {
                LOG.debug("Polling and updating message info for message index {}", this.cachedMessageIndex);
            }
            final EqHashTable<TopicPartition, Long> map = EqHashTable.New();
            for (final var partition : consumer.assignment())
            {
                // since we don't know the exact offset for each partition we just subtract the amount of
                // missing cached packets to ensure that we read them again after the backup node restarts
                long offset = consumer.position(partition);
                offset = Math.max(offset - this.cachedPackets.size(), 0);
                map.put(partition, offset);
            }
            final var info = MessageInfo.New(this.cachedMessageIndex, map.immure());
            this.messageInfo.set(info);
            return info;
        }

        /**
         * Polls the kafka consumer and consumes fully completed messages. Invalid packets are skipped and incomplete
         * messages will be cached.
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

            final var packets = new ArrayList<ClusterStorageBinaryDataPacket>(this.cachedPackets.size());

            outer:
            while (!this.cachedPackets.isEmpty())
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
                    break;
                }

                final var newMessagePackets = new ArrayList<ClusterStorageBinaryDataPacket>(rootPacket.packetCount());

                for (int i = 0; i < rootPacket.packetCount(); i++)
                {
                    final var packet = this.cachedPackets.peek();

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

                    newMessagePackets.add(this.cachedPackets.remove());
                }

                packets.addAll(newMessagePackets);
            }

            this.consumeFullMessage(packets, consumer);
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
            final Collection<ClusterStorageBinaryDataPacket> packets,
            final KafkaConsumer<String, byte[]> consumer
        )
        {
            final List<StorageBinaryDataPacket> newPackets = new ArrayList<>(packets.size());

            for (final var packet : packets)
            {
                if (this.cachedMessageIndex >= packet.messageIndex())

                {
                    LOG.warn(
                        "Skipping packet with offset {} (current: {})",
                        packet.messageIndex(),
                        this.cachedMessageIndex
                    );
                    continue;
                }

                this.cachedMessageIndex = packet.messageIndex();

                if (LOG.isTraceEnabled() && this.cachedMessageIndex % 10_000 == 0)
                {
                    LOG.trace("Consuming packet with offset {}", this.cachedMessageIndex);
                }

                newPackets.add(packet);

            }

            if (!newPackets.isEmpty())
            {
                if (LOG.isDebugEnabled() && this.cachedMessageIndex % 10_000 == 0)
                {
                    LOG.debug("Applying packets at offset {}", this.cachedMessageIndex);
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
                ClusterStorageBinaryDistributedKafka.messageIndex(headers),
                ByteBuffer.wrap(record.value())
            );
        }

        /**
         * Stop collecting updates after the last available offset in kafka has been reached.
         */
        @Override
        public void stopAtLatestMessage()
        {
            LOG.info("DataClient will stop at latest offset");
            this.stopAtLatestMessage.set(true);
        }

        @Override
        public void resume() throws NodelibraryException
        {
            if (this.stopAtLatestMessage.get())
            {
                throw new NodelibraryException(
                    new IllegalStateException("Client is sill reading up to the latest message index")
                );
            }
            if (this.isRunning())
            {
                throw new NodelibraryException(new IllegalStateException("Client is sill active"));
            }
            this.start();
        }

        @Override
        public void dispose()
        {
            LOG.trace("Disposing data client");
            this.requestStop.set(true);
            while (this.isRunning())
            {
                try
                {
                    LOG.trace("Waiting for runner to stop");
                    this.runner.join(1_000L);
                }
                catch (final InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new NodelibraryException(e);
                }
            }
            this.packetAcceptor.dispose();
            this.offsetChangedListener.close();
        }
    }
}
