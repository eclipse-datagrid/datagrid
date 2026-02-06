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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.serializer.Serializer;
import org.eclipse.serializer.SerializerFoundation;
import org.eclipse.serializer.util.X;
import org.eclipse.store.cache.hibernate.types.CacheRegionFactory;
import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.internal.BasicCacheKeyImplementation;
import org.hibernate.cache.internal.CacheKeyImplementation;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.support.DomainDataStorageAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

public class ClusteredCacheRegionFactory extends CacheRegionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(ClusteredCacheRegionFactory.class);

    private final CacheEntryListenerConfiguration<Object, Object> cacheEntryListenerConfiguration;
    private final CacheInvalidationReceiver cacheInvalidationReceiver;
    private final CacheInvalidationSender<Object, Object> cacheInvalidationSender;

    private String timestampsRegionCacheName;

    public ClusteredCacheRegionFactory()
    {
        this(DefaultCacheKeysFactory.INSTANCE);
    }

    public ClusteredCacheRegionFactory(final CacheKeysFactory cacheKeysFactory)
    {
        super(cacheKeysFactory);

        final var topicName = "cache-invalidation";
        final var serializer = Serializer.Bytes(SerializerFoundation.New().registerEntityTypes(
            UUID.class,
            CacheKeyImplementation.class, BasicCacheKeyImplementation.class,
            InvalidationMessage.class, FullInvalidationMessage.class
        ));
        final var clientId = UUID.randomUUID().toString();

        final var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        final var kafkaProducer = new KafkaProducer<String, byte[]>(properties);
        this.cacheInvalidationSender = new CacheInvalidationSender<>(
            kafkaProducer,
            topicName,
            clientId,
            serializer,
            () -> this.timestampsRegionCacheName
        );

        final var kafkaConsumer = new KafkaConsumer<String, byte[]>(properties);
        final Consumer<InvalidationMessage> onInvalidationMessageReceived = message ->
        {
            final var cacheManager = X.notNull(this.getCacheManager());
            final var cache = cacheManager.getCache(message.cacheName());
            if (cache == null)
            {
                return;
            }
            if (cache.getName().equals(this.timestampsRegionCacheName))
            {
                final var timestampsMessage = (FullInvalidationMessage)message;
                System.out.printf(
                    "Adding new timestamp for %s %s %n",
                    timestampsMessage.key(),
                    timestampsMessage.value()
                );
                cache.putSilent(timestampsMessage.key(), timestampsMessage.value());
            }
            else
            {
                System.out.printf("Removing cache=%s, key=%s %n", message.cacheName(), message.key());
                cache.remove(message.key());
            }
        };

        this.cacheInvalidationReceiver = new CacheInvalidationReceiver(
            kafkaConsumer,
            clientId,
            serializer,
            onInvalidationMessageReceived,
            topicName
        );
        this.cacheEntryListenerConfiguration = new ClusteredCacheEntryListenerConfiguration<>(cacheInvalidationSender);

        cacheInvalidationReceiver.start();
    }

    @Override
    protected StorageAccess createTimestampsRegionStorageAccess(
        final String regionName,
        final SessionFactoryImplementor sessionFactory
    )
    {
        this.timestampsRegionCacheName = this.defaultRegionName(
            regionName,
            sessionFactory,
            DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME,
            LEGACY_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAMES
        );
        final var cache = this.getOrCreateCache(this.timestampsRegionCacheName, sessionFactory);
        cache.registerCacheEntryListener(this.cacheEntryListenerConfiguration);
        return StorageAccess.New(cache);
    }

    @Override
    protected DomainDataStorageAccess createDomainDataStorageAccess(
        final DomainDataRegionConfig regionConfig,
        final DomainDataRegionBuildingContext buildingContext
    )
    {
        final var cache = this.getOrCreateCache(regionConfig.getRegionName(), buildingContext.getSessionFactory());
        cache.registerCacheEntryListener(this.cacheEntryListenerConfiguration);
        return StorageAccess.New(cache);
    }

    @Override
    protected void releaseFromUse()
    {
        try
        {
            this.cacheInvalidationSender.close();
        }
        catch (final Exception e)
        {
            logger.error("Failed to close invalidation sender.", e);
        }

        try
        {
            this.cacheInvalidationReceiver.close();
        }
        catch (final Exception e)
        {
            logger.error("Failed to close invalidation receiver.", e);
        }

        super.releaseFromUse();
    }
}
