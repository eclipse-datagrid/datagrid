package org.eclipse.datagrid.cache.clustered.kafka.types;

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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessage;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageAcceptor;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageReceiver;
import org.eclipse.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClusteredCacheMessageReceiver implements ClusteredCacheMessageReceiver
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaClusteredCacheMessageReceiver.class);
    private final Properties kafkaProperties;
    private final String topicName;
    private final String clientId;
    private final ClusteredCacheMessageAcceptor messageAcceptor;
    private final Serializer<byte[]> serializer;

    private boolean started = false;
    private final AtomicBoolean active = new AtomicBoolean(false);
    private Thread thread;

    public KafkaClusteredCacheMessageReceiver(
        final Properties kafkaProperties,
        final String topicName,
        final String clientId,
        final ClusteredCacheMessageAcceptor messageAcceptor,
        final Serializer<byte[]> serializer
    )
    {
        this.kafkaProperties = kafkaProperties;
        this.topicName = topicName;
        this.clientId = clientId;
        this.messageAcceptor = messageAcceptor;
        this.serializer = serializer;
    }

    @Override
    public void start() throws IllegalStateException
    {
        if (this.started)
        {
            throw new IllegalStateException("Receiver is already running");
        }

        this.thread = new Thread(this::run);
        this.thread.start();

        this.started = true;
        this.active.set(true);
    }

    private void run()
    {
        final Properties properties = new Properties();
        properties.putAll(this.kafkaProperties);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.clientId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (final var consumer = new KafkaConsumer<String, byte[]>(properties))
        {
            consumer.subscribe(Collections.singleton(this.topicName));
            while (this.active.get())
            {
                final ConsumerRecords<String, byte[]> records;

                try
                {
                    records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                }
                catch (final InterruptException e)
                {
                    // clear interrupt flag so Kafka can properly close the consumer
                    final var ignored = Thread.interrupted();
                    continue;
                }

                this.consume(records);

                try
                {
                    consumer.commitSync();
                }
                catch (final InterruptException e)
                {
                    // clear interrupt flag so Kafka can properly close the consumer
                    final var ignored = Thread.interrupted();
                }
            }
        }
    }

    private void consume(final ConsumerRecords<String, byte[]> records)
    {
        for (final var record : records)
        {
            final String senderId = new String(
                record.headers().lastHeader("Sender-Id").value(),
                StandardCharsets.UTF_8
            );
            if (senderId.equals(this.clientId))
            {
                continue;
            }
            final ClusteredCacheMessage message = this.serializer.deserialize(record.value());
            this.messageAcceptor.accept(message);
        }
    }

    @Override
    public void dispose()
    {
        this.active.set(false);

        if (this.thread != null)
        {
            this.thread.interrupt();

            try
            {
                this.thread.join(1_000L);
            }
            catch (final InterruptedException e)
            {
                // ignored
            }

            if (this.thread.isAlive())
            {
                logger.warn("Failed to wait for receiver thread to finish.");
            }
        }
    }
}
