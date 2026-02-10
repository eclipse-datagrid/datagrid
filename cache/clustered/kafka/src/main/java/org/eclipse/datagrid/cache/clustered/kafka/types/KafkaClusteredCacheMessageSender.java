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

import java.util.Properties;
import javax.cache.event.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.eclipse.datagrid.cache.clustered.types.CacheInvalidationMessage;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessage;
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
        final Properties kafkaProperties,
        final String topicName,
        final String clientId,
        final Serializer<byte[]> serializer
    )
    {
        return new UpdateTimestamps<>(
            notNull(kafkaProperties),
            notNull(topicName),
            notNull(clientId),
            notNull(serializer)
        );
    }

    static <K, V> ClusteredCacheMessageSender<K, V> CacheInvalidation(
        final Properties kafkaProperties,
        final String topicName,
        final String clientId,
        final Serializer<byte[]> serializer
    )
    {
        return new CacheInvalidation<>(
            notNull(kafkaProperties),
            notNull(topicName),
            notNull(clientId),
            notNull(serializer)
        );
    }

    abstract class Abstract<K, V> implements ClusteredCacheMessageSender<K, V>
    {
        private static final Logger logger = LoggerFactory.getLogger(KafkaClusteredCacheMessageSender.Abstract.class);
        private final Properties kafkaProperties;
        private final String topicName;
        private final String clientId;
        private final Serializer<byte[]> serializer;

        private KafkaProducer<String, byte[]> producer;

        protected Abstract(
            final Properties kafkaProperties,
            final String topicName, final String clientId,
            final Serializer<byte[]> serializer
        )
        {
            this.kafkaProperties = kafkaProperties;
            this.topicName = topicName;
            this.clientId = clientId;
            this.serializer = serializer;
        }

        private KafkaProducer<String, byte[]> ensureProducer()
        {
            if (this.producer == null)
            {
                final Properties properties = new Properties();
                properties.putAll(this.kafkaProperties);
                properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class.getName());
                properties.setProperty(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName()
                );
                this.producer = new KafkaProducer<>(properties);
            }
            return this.producer;
        }

        protected abstract ClusteredCacheMessage createMessage(CacheEntryEvent<? extends K, ? extends V> event);

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
            final var producer = this.ensureProducer();
            final var future = producer.send(record);
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
            final Properties kafkaProperties,
            final String topicName,
            final String clientId,
            final Serializer<byte[]> serializer
        )
        {
            super(kafkaProperties, topicName, clientId, serializer);
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
        protected ClusteredCacheMessage createMessage(final CacheEntryEvent<? extends K, ? extends V> event)
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

    class CacheInvalidation<K, V> extends Abstract<K, V> implements CacheEntryUpdatedListener<K, V>
    {
        public CacheInvalidation(
            final Properties kafkaProperties,
            final String topicName,
            final String clientId,
            final Serializer<byte[]> serializer
        )
        {
            super(kafkaProperties, topicName, clientId, serializer);
        }

        @Override
        public void onUpdated(final Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
            throws CacheEntryListenerException
        {
            this.handleEvents(cacheEntryEvents);
        }

        @Override
        protected ClusteredCacheMessage createMessage(final CacheEntryEvent<? extends K, ? extends V> event)
        {
            return new CacheInvalidationMessage(event.getSource().getName(), event.getKey());
        }
    }
}
