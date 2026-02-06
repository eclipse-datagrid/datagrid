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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.serializer.Serializer;

import javax.cache.event.*;
import java.util.function.Supplier;

public class CacheInvalidationSender<K, V>
    implements CacheEntryUpdatedListener<K, V>, CacheEntryCreatedListener<K, V>, AutoCloseable
{
    private final KafkaProducer<String, byte[]> kafkaProducer;
    private final String topicName;
    private final String clientId;
    private final Serializer<byte[]> serializer;
    private final Supplier<String> timestampsRegionNameSupplier;

    public CacheInvalidationSender(
        final KafkaProducer<String, byte[]> kafkaProducer,
        final String topicName, final String clientId,
        final Serializer<byte[]> serializer,
        final Supplier<String> timestampsRegionNameSupplier
    )
    {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.clientId = clientId;
        this.serializer = serializer;
        this.timestampsRegionNameSupplier = timestampsRegionNameSupplier;
    }

    @Override
    public void onUpdated(final Iterable<CacheEntryEvent<? extends K, ? extends V>> events)
        throws CacheEntryListenerException
    {
        this.handleEvents(events);
    }

    @Override
    public void onCreated(final Iterable<CacheEntryEvent<? extends K, ? extends V>> events)
        throws CacheEntryListenerException
    {
        this.handleEvents(events);
    }

    private void handleEvents(final Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
    {
        for (final var event : cacheEntryEvents)
        {
            // only handle created events for the timestamps cache
            if (event.getEventType() == EventType.CREATED
                && !event.getSource().getName().equals(this.timestampsRegionNameSupplier.get()))
            {
                continue;
            }

            final var cacheName = event.getSource().getName();
            final var key = event.getKey();
            final var value = event.getValue();

            System.out.printf("%s { Cache='%s', Key='%s' }%n", event.getEventType(), cacheName, key);

            final InvalidationMessage message = cacheName.equals(this.timestampsRegionNameSupplier.get())
                                                ? new FullInvalidationMessage(cacheName, key, value)
                                                : new InvalidationMessage(cacheName, key);

            final var record = new ProducerRecord<String, byte[]>(this.topicName, this.serializer.serialize(message));
            record.headers().add("Sender", this.serializer.serialize(this.clientId));

            System.out.println("Sending key of type " + message.key().getClass().getName());
            this.sendRecord(record);
        }
    }

    private void sendRecord(final ProducerRecord<String, byte[]> record)
    {
        final var future = this.kafkaProducer.send(record);
        try
        {
            future.get();
        }
        catch (final Exception e)
        {
            throw new CacheEntryListenerException(e);
        }
    }

    @Override
    public void close()
    {
        this.kafkaProducer.close();
    }
}
