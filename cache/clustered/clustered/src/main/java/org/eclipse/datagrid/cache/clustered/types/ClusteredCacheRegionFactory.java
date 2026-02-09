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
import org.eclipse.serializer.Serializer;
import org.eclipse.serializer.SerializerFoundation;
import org.eclipse.store.cache.hibernate.types.CacheRegionFactory;
import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.support.DomainDataStorageAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class ClusteredCacheRegionFactory extends CacheRegionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(ClusteredCacheRegionFactory.class);

    private ClusteredCacheEntryListenerConfiguration<Object, Object> cacheEntryListenerConfiguration;
    private ClusteredCacheMessageReceiver cacheMessageReceiver;

    public ClusteredCacheRegionFactory()
    {
        this(DefaultCacheKeysFactory.INSTANCE);
    }

    public ClusteredCacheRegionFactory(final CacheKeysFactory cacheKeysFactory)
    {
        super(cacheKeysFactory);
    }

    @Override
    protected void prepareForUse(final SessionFactoryOptions settings, final Map cacheProperties)
    {
        super.prepareForUse(settings, cacheProperties);

        final var typesProvider = this.resolveSerializationTypesProvider(settings, cacheProperties);
        final var topicName = "cache-invalidation";
        final var serializer =
            Serializer.Bytes(SerializerFoundation.New().registerEntityTypes(typesProvider.provideTypes()));
        final var clientId = UUID.randomUUID().toString();

        final var kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final var cacheManager = this.resolveCacheManager(settings, cacheProperties);
        final var messageAcceptor = new ClusteredCacheMessageAcceptor(cacheManager);
        this.cacheMessageReceiver = new ClusteredCacheMessageReceiver(
            kafkaProperties,
            topicName,
            clientId,
            messageAcceptor,
            serializer
        );
        this.cacheEntryListenerConfiguration = new ClusteredCacheEntryListenerConfiguration<>(
            ClusteredCacheMessageSender.UpdateTimestamps(kafkaProperties, topicName, clientId, serializer),
            ClusteredCacheMessageSender.CacheInvalidation(kafkaProperties, topicName, clientId, serializer)
        );

        cacheMessageReceiver.start();
    }

    @Override
    protected StorageAccess createTimestampsRegionStorageAccess(
        final String regionName,
        final SessionFactoryImplementor sessionFactory
    )
    {
        final String defaultedRegionName = this.defaultRegionName(
            regionName,
            sessionFactory,
            DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME,
            LEGACY_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAMES
        );
        final var cache = this.getOrCreateCache(defaultedRegionName, sessionFactory);
        cache.registerCacheEntryListener(this.cacheEntryListenerConfiguration.getUpdateTimestampsCacheEntryListenerConfiguration());
        return StorageAccess.New(cache);
    }

    @Override
    protected DomainDataStorageAccess createDomainDataStorageAccess(
        final DomainDataRegionConfig regionConfig,
        final DomainDataRegionBuildingContext buildingContext
    )
    {
        final var cache = this.getOrCreateCache(regionConfig.getRegionName(), buildingContext.getSessionFactory());
        cache.registerCacheEntryListener(this.cacheEntryListenerConfiguration.getCacheInvalidationCacheEntryListenerConfiguration());
        return StorageAccess.New(cache);
    }

    @SuppressWarnings("unchecked")
    protected SerializationTypesProvider resolveSerializationTypesProvider(
        final SessionFactoryOptions settings,
        @SuppressWarnings("rawtypes") // superclass uses raw type
        final Map properties
    )
    {
        final Object setting = properties.get(ConfigurationPropertyNames.SERIALIZATION_TYPES_PROVIDER);
        if (setting == null)
        {
            return new SerializationTypesProvider.Default();
        }
        if (setting instanceof final SerializationTypesProvider p)
        {
            return p;
        }

        try
        {
            final Class<? extends SerializationTypesProvider> typesProviderClass;
            if (setting instanceof Class)
            {
                typesProviderClass = (Class<? extends SerializationTypesProvider>)setting;
            }
            else
            {
                typesProviderClass = this.loadClass(setting.toString(), settings);
            }
            return typesProviderClass.getDeclaredConstructor().newInstance();
        }
        catch (final ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
            InvocationTargetException e)
        {
            throw new CacheException("Could not use explicit CacheManager : " + setting, e);
        }
    }

    @Override
    protected void releaseFromUse()
    {
        try
        {
            this.cacheEntryListenerConfiguration.dispose();
        }
        catch (final Exception e)
        {
            logger.error("Failed to close entry listeners.", e);
        }

        try
        {
            this.cacheMessageReceiver.dispose();
        }
        catch (final Exception e)
        {
            logger.error("Failed to close invalidation receiver.", e);
        }

        super.releaseFromUse();
    }
}
