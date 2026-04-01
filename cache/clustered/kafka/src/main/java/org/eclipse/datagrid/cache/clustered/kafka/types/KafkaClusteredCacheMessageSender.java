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

import javax.cache.event.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageSender;
import org.eclipse.datagrid.cache.clustered.types.TimestampsRegionUpdateMessage;
import org.eclipse.serializer.Serializer;
import org.eclipse.serializer.typing.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface KafkaClusteredCacheMessageSender<K, V> extends CacheEntryListener<K, V>, Disposable
{
    static <K, V> ClusteredCacheMessageSender<K, V> UpdateTimestamps(
        final KafkaProducer<String, byte[]> producer,
        final String topicName,
        final String clientId,
        final Serializer<byte[]> serializer
    )
    {
        return new UpdateTimestamps<>(
            notNull(producer),
            notNull(topicName),
            notNull(clientId),
            notNull(serializer)
        );
    }

    abstract class Abstract<K, V> implements ClusteredCacheMessageSender<K, V>
    {
        private static final Logger logger = LoggerFactory.getLogger(KafkaClusteredCacheMessageSender.Abstract.class);
        private final KafkaProducer<String, byte[]> producer;
        private final String topicName;
        private final String clientId;
        private final Serializer<byte[]> serializer;

        protected Abstract(
            final KafkaProducer<String, byte[]> producer,
            final String topicName, final String clientId,
            final Serializer<byte[]> serializer
        )
        {
            this.producer = producer;
            this.topicName = topicName;
            this.clientId = clientId;
            this.serializer = serializer;
        }

        protected abstract TimestampsRegionUpdateMessage createMessage(CacheEntryEvent<? extends K, ? extends V> event);

        protected void handleEvents(final Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
            throws CacheEntryListenerException
        {
            for (final var event : cacheEntryEvents)
            {
                final var cacheName = event.getSource().getName();
                final var key = event.getKey();
                logger.debug("Sending cache message cache={}, key={}", cacheName, key);

                final byte[] message;
                try
                {
                    message = this.serializer.serialize(this.createMessage(event));
                }
                catch (final Exception e)
                {
                    throw new CacheEntryListenerException("Failed to serialize message", e);
                }

                final var record = new ProducerRecord<String, byte[]>(this.topicName, message);
                record.headers().add("Sender-Id", this.serializer.serialize(this.clientId));

                this.sendRecord(record);
            }
        }

        private void sendRecord(final ProducerRecord<String, byte[]> record) throws CacheEntryListenerException
        {
            final var future = this.producer.send(record);
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
        public void dispose()
        {
            if (this.producer != null)
            {
                try
                {
                    this.producer.close();
                }
                catch (final RuntimeException e)
                {
                    logger.error("Failed to close Kafka producer.", e);
                }
            }
        }
    }

    class UpdateTimestamps<K, V> extends Abstract<K, V>
        implements CacheEntryUpdatedListener<K, V>, CacheEntryCreatedListener<K, V>
    {
        public UpdateTimestamps(
            final KafkaProducer<String, byte[]> producer,
            final String topicName,
            final String clientId,
            final Serializer<byte[]> serializer
        )
        {
            super(producer, topicName, clientId, serializer);
        }

        @Override
        public void onCreated(final Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
            throws CacheEntryListenerException
        {
            this.handleEvents(cacheEntryEvents);
        }

        @Override
        public void onUpdated(final Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
            throws CacheEntryListenerException
        {
            this.handleEvents(cacheEntryEvents);
        }

        @Override
        protected TimestampsRegionUpdateMessage createMessage(final CacheEntryEvent<? extends K, ? extends V> event)
        {
            final var eventKey = event.getKey();
            final var eventValue = event.getValue();
            if (!(eventKey instanceof final String key))
            {
                throw new IllegalArgumentException("Event key is not of type " + String.class.getName());
            }
            if (!(eventValue instanceof final Long value))
            {
                throw new IllegalArgumentException("Event value is not of type " + Long.class.getName());
            }
            return new TimestampsRegionUpdateMessage(event.getSource().getName(), key, value);
        }
    }
}
