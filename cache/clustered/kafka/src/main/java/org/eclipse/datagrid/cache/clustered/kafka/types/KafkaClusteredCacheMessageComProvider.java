package org.eclipse.datagrid.cache.clustered.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered Kafka
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

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageAcceptor;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageComProvider;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageReceiver;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageSender;
import org.eclipse.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClusteredCacheMessageComProvider<K, V> implements ClusteredCacheMessageComProvider<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaClusteredCacheMessageComProvider.class);

    private String clientId;
    private KafkaProducer<String, byte[]> producer;

    @Override
    public ClusteredCacheMessageSender<K, V> provideUpdateTimestampsCacheMessageSender(
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer
    )
    {
        final var producer = this.ensureProducer(properties);
        final var topicName = this.getTopicName(properties);
        final var clientId = this.ensureClientId();
        return KafkaClusteredCacheMessageSender.UpdateTimestamps(producer, topicName, clientId, serializer);
    }

    @Override
    public ClusteredCacheMessageReceiver provideMessageReceiver(
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer,
        final ClusteredCacheMessageAcceptor messageAcceptor
    )
    {
        final var kafkaProperties = this.readKafkaConfigProperties(
            properties,
            KafkaClusteredConfigurationPropertyNames.KAFKA_CONSUMER_CONFIG_PREFIX
        );
        final var topicName = this.getTopicName(properties);
        final var clientId = this.ensureClientId();
        return new KafkaClusteredCacheMessageReceiver(
            kafkaProperties,
            topicName,
            clientId,
            messageAcceptor,
            serializer
        );
    }

    private String ensureClientId()
    {
        if (this.clientId == null)
        {
            this.clientId = UUID.randomUUID().toString();
        }
        return this.clientId;
    }

    private KafkaProducer<String, byte[]> ensureProducer(@SuppressWarnings("rawtypes") final Map properties)
    {
        if (this.producer == null)
        {
            final var kafkaProperties = this.readKafkaConfigProperties(
                properties,
                KafkaClusteredConfigurationPropertyNames.KAFKA_PRODUCER_CONFIG_PREFIX
            );
            kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class.getName());
            kafkaProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName()
            );
            this.producer = new KafkaProducer<>(kafkaProperties);
        }
        return this.producer;
    }

    private String getTopicName(@SuppressWarnings("rawtypes") final Map properties)
    {
        final var topicName = (String)properties.get(KafkaClusteredConfigurationPropertyNames.TOPIC);
        return topicName != null ? topicName : "es-cache-invalidation";
    }

    private Properties readKafkaConfigProperties(
        @SuppressWarnings("rawtypes") final Map rawProperties,
        final String specificPrefix
    )
    {
        final Properties kafkaProperties = new Properties();
        for (final var rawKey : rawProperties.keySet())
        {
            if (rawKey instanceof final String prefixedKey)
            {
                final var key = this.removePrefixIfConfigKey(prefixedKey, specificPrefix);
                if (key != null)
                {
                    final var value = rawProperties.get(rawKey);
                    logger.trace("Found Kafka config with key={}, value={}", key, value);
                    kafkaProperties.put(key, value);
                }
            }
        }
        return kafkaProperties;
    }

    private String removePrefixIfConfigKey(final String key, final String specificPrefix)
    {
        final var prefixes = new String[] {
            specificPrefix,
            KafkaClusteredConfigurationPropertyNames.KAFKA_CONFIG_PREFIX
        };
        for (final var prefix : prefixes)
        {
            if (key.startsWith(prefix))
            {
                return key.substring(prefix.length());
            }
        }
        return null;
    }
}
