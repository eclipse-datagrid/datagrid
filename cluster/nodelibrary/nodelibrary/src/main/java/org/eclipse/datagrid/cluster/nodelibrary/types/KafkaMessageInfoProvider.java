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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.serializer.collections.EqHashTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Locale;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.eclipse.serializer.util.X.notNull;

/**
 * Tool to ask kafka for the last message info available
 */
public class KafkaMessageInfoProvider implements AutoCloseable
{
    public static KafkaMessageInfoProvider New(
        final String topic,
        final String groupInstanceId,
        final KafkaPropertiesProvider kafkaPropertiesProvider
    )
    {
        return new KafkaMessageInfoProvider(
            notNull(topic),
            notNull(groupInstanceId),
            notNull(kafkaPropertiesProvider)
        );
    }

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageInfoProvider.class);
    private static final long PARTITION_ASSIGNMENT_TIMEOUT_MS = 10_000L;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(250L);

    private final KafkaConsumer<String, byte[]> kafka;
    private final KafkaPropertiesProvider kafkaPropertiesProvider;
    private final String topic;

    private KafkaMessageInfoProvider(
        final String topic,
        final String groupInstanceId,
        final KafkaPropertiesProvider kafkaPropertiesProvider
    )
    {
        this.topic = topic;
        this.kafkaPropertiesProvider = kafkaPropertiesProvider;
        this.kafka = this.createKafkaConsumer(groupInstanceId);
    }

    private KafkaConsumer<String, byte[]> createKafkaConsumer(final String groupInstanceId)
    {
        final var properties = this.kafkaPropertiesProvider.provide();
        properties.setProperty(GROUP_ID_CONFIG, groupInstanceId);
        properties.setProperty(GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        properties.setProperty(CLIENT_ID_CONFIG, groupInstanceId);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        properties.setProperty(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
        return new KafkaConsumer<>(properties);
    }

    @Override
    public void close() throws KafkaException
    {
        LOG.trace("Closing KafkaMessageInfoProvider");
        this.kafka.close();
    }

    public void init() throws KafkaException
    {
        LOG.trace("Initializing KafkaMessageInfoProvider. Subscribing to topic {}", this.topic);
        this.kafka.subscribe(Collections.singleton(this.topic));

        LOG.trace("Polling consumer until we have partitions assigned.");
        final long startMs = System.currentTimeMillis();
        final long endMs = startMs + PARTITION_ASSIGNMENT_TIMEOUT_MS;
        while (this.kafka.assignment().isEmpty())
        {
            if (System.currentTimeMillis() > endMs)
            {
                throw new RuntimeException("Timed out waiting for topic partition assignment");
            }
            this.kafka.poll(POLL_TIMEOUT);
        }
    }

    /**
     * Creates a new kafka consumer and asks for the last message in the topic, returns the message index of that
     * message
     */
    public long provideLatestMessageIndex() throws KafkaException
    {
        long lastMessageIndex = Long.MIN_VALUE;

        this.seekToLastOffsets();

        //		LOG.trace("Polling latest messages for topic {}", this.topic);
        for (final var rec : this.kafka.poll(POLL_TIMEOUT))
        {
            final long messageIndex = ClusterStorageBinaryDistributedKafka.deserializeLong(
                rec.headers().lastHeader(ClusterStorageBinaryDistributedKafka.keyMessageIndex()).value()
            );
            if (messageIndex > lastMessageIndex)
            {
                lastMessageIndex = messageIndex;
            }
        }

        return lastMessageIndex;
    }

    public MessageInfo provideLatestMessageInfo()
    {
        final var messageIndex = this.provideLatestMessageIndex();

        final EqHashTable<TopicPartition, Long> kafkaOffsets = EqHashTable.New();
        for (final var partition : this.kafka.assignment())
        {
            final long offset = this.kafka.position(partition);
            kafkaOffsets.put(partition, offset);
        }

        return MessageInfo.New(messageIndex, kafkaOffsets.immure());
    }

    private void seekToLastOffsets() throws KafkaException
    {
        LOG.trace("Seeking to last partition messages");
        this.kafka.poll(POLL_TIMEOUT); // Polling to update assignments
        final var partitions = this.kafka.assignment();
        this.kafka.seekToEnd(partitions);
        for (final var partition : partitions)
        {
            final long endOffset = this.kafka.position(partition);
            if (endOffset > 0)
            {
                this.kafka.seek(partition, endOffset - 1);
            }
        }
    }
}
