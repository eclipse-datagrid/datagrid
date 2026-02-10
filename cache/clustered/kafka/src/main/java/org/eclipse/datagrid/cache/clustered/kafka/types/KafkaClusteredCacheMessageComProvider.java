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

import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageAcceptor;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageComProvider;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageReceiver;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageSender;
import org.eclipse.serializer.Serializer;

public class KafkaClusteredCacheMessageComProvider<K, V> implements ClusteredCacheMessageComProvider<K, V>
{
    // TODO (MS 10.02.2026): Use same KafkaProducer for the message senders and the same client id for everything.

    // NOTE: this list needs to go from most specific to the least specific prefix
    private static final String[] PRODUCER_CONFIG_PREFIXES = new String[] {
        KafkaClusteredConfigurationPropertyNames.KAFKA_PRODUCER_CONFIG_PREFIX,
        KafkaClusteredConfigurationPropertyNames.KAFKA_CONFIG_PREFIX
    };

    @Override
    public ClusteredCacheMessageSender<K, V> provideUpdateTimestampsCacheMessageSender(
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer
    )
    {
        final var kafkaProperties = this.readKafkaConfigProperties(properties);
        final var topicName = this.getTopicName(properties);
        final var clientId = UUID.randomUUID().toString();
        return KafkaClusteredCacheMessageSender.UpdateTimestamps(kafkaProperties, topicName, clientId, serializer);
    }

    @Override
    public ClusteredCacheMessageSender<K, V> provideUpdateCacheInvalidationMessageSender(
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer
    )
    {
        final var kafkaProperties = this.readKafkaConfigProperties(properties);
        final var topicName = this.getTopicName(properties);
        final var clientId = UUID.randomUUID().toString();
        return KafkaClusteredCacheMessageSender.CacheInvalidation(kafkaProperties, topicName, clientId, serializer);
    }

    @Override
    public ClusteredCacheMessageReceiver provideMessageReceiver(
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer,
        final ClusteredCacheMessageAcceptor messageAcceptor
    )
    {
        final var kafkaProperties = this.readKafkaConfigProperties(properties);
        final var topicName = this.getTopicName(properties);
        final var clientId = UUID.randomUUID().toString();
        return new KafkaClusteredCacheMessageReceiver(
            kafkaProperties,
            topicName,
            clientId,
            messageAcceptor,
            serializer
        );
    }

    private String getTopicName(@SuppressWarnings("rawtypes") final Map properties)
    {
        final var topicName = (String)properties.get(KafkaClusteredConfigurationPropertyNames.TOPIC);
        return topicName != null ? topicName : "es-cache-invalidation";
    }

    private Properties readKafkaConfigProperties(@SuppressWarnings("rawtypes") final Map rawProperties)
    {
        final Properties kafkaProperties = new Properties();
        for (final var rawKey : rawProperties.keySet())
        {
            if (rawKey instanceof final String prefixedKey)
            {
                final var key = this.removePrefixIfProducerConfigKey(prefixedKey);
                if (key != null)
                {
                    final var value = rawProperties.get(rawKey);
                    kafkaProperties.put(key, value);
                }
            }
        }
        return kafkaProperties;
    }

    private String removePrefixIfProducerConfigKey(final String key)
    {
        for (final var prefix : PRODUCER_CONFIG_PREFIXES)
        {
            if (key.startsWith(prefix))
            {
                return key.substring(prefix.length());
            }
        }
        return null;
    }
}
