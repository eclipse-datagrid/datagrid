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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.serializer.Serializer;

import java.time.Duration;
import java.util.Collections;
import java.util.function.Consumer;

public class CacheInvalidationReceiver extends Thread implements AutoCloseable
{
    private final KafkaConsumer<String, byte[]> kafkaConsumer;
    private final String clientId;
    private final Serializer<byte[]> serializer;
    private final Consumer<InvalidationMessage> invalidationMessageConsumer;
    private final String topicName;

    private volatile boolean running = true;

    public CacheInvalidationReceiver(
        final KafkaConsumer<String, byte[]> kafkaConsumer,
        final String clientId,
        final Serializer<byte[]> serializer,
        final Consumer<InvalidationMessage> invalidationMessageConsumer,
        final String topicName
    )
    {
        this.kafkaConsumer = kafkaConsumer;
        this.clientId = clientId;
        this.serializer = serializer;
        this.invalidationMessageConsumer = invalidationMessageConsumer;
        this.topicName = topicName;
    }

    @Override
    public void run()
    {
        this.kafkaConsumer.subscribe(Collections.singleton(this.topicName));
        while (this.running)
        {
            final var records = this.kafkaConsumer.poll(Duration.ofMillis(1000));
            for (final var record : records)
            {
                final var serializedSenderHeader = record.headers().lastHeader("Sender").value();
                if (this.serializer.deserialize(serializedSenderHeader).equals(this.clientId))
                {
                    // skip our own messages
                    continue;
                }
                final InvalidationMessage message = this.serializer.deserialize(record.value());
                this.invalidationMessageConsumer.accept(message);
            }
        }
    }

    @Override
    public void close()
    {
        this.running = false;
        this.kafkaConsumer.close();
    }
}
