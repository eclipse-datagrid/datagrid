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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.datagrid.storage.distributed.types.ObjectMaterializer;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMerger;
import org.eclipse.serializer.collections.types.XEnum;
import org.eclipse.serializer.collections.types.XList;
import org.eclipse.serializer.concurrency.XThreads;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDescription;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.typing.Disposable;
import org.eclipse.serializer.util.X;
import org.eclipse.serializer.util.logging.Logging;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

import static org.eclipse.serializer.math.XMath.notNegative;
import static org.eclipse.serializer.util.X.notNull;


public interface ClusterStorageBinaryDataMerger extends StorageBinaryDataMerger, Disposable
{
    static ClusterStorageBinaryDataMerger New(
        final BinaryPersistenceFoundation<?> foundation,
        final StorageConnection storage,
        final ObjectGraphUpdateHandler objectGraphUpdateHandler,
        final long cachingTimeoutMs
    )
    {
        return new Default(
            notNull(foundation),
            notNull(storage),
            notNull(objectGraphUpdateHandler),
            notNegative(cachingTimeoutMs)
        );
    }

    interface Defaults
    {
        static long cachingTimeoutMs()
        {
            return 10_000L;
        }
    }

    class Default implements ClusterStorageBinaryDataMerger
    {
        private static final Logger LOG = Logging.getLogger(ClusterStorageBinaryDataMerger.class);

        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final ConcurrentLinkedQueue<XEnum<ByteBuffer>> cachedData = new ConcurrentLinkedQueue<>();

        private final BinaryPersistenceFoundation<?> foundation;
        private final StorageConnection storage;
        private final ObjectGraphUpdateHandler objectGraphUpdateHandler;
        private final long cachingTimeoutMs;

        private Future<?> updateFuture = CompletableFuture.completedFuture(null);

        private Default(
            final BinaryPersistenceFoundation<?> foundation,
            final StorageConnection storage,
            final ObjectGraphUpdateHandler objectGraphUpdateHandler,
            final long cachingTimeoutMs
        )
        {
            this.foundation = foundation;
            this.storage = storage;
            this.objectGraphUpdateHandler = objectGraphUpdateHandler;
            this.cachingTimeoutMs = cachingTimeoutMs;
        }

        @Override
        public synchronized void receiveData(final Binary data)
        {
            final XEnum<ByteBuffer> dataEnum = X.Enum(data.buffers());
            this.storage.importData(dataEnum);
            this.cachedData.add(dataEnum);

            if (this.updateFuture.isDone())
            {
                this.updateFuture = this.executor.submit(() ->
                {
                    try
                    {
                        XThreads.sleep(this.cachingTimeoutMs);
                        this.applyData();
                    }
                    catch (final Throwable t)
                    {
                        GlobalErrorHandling.handleFatalError(t);
                    }
                });
            }
        }

        private void applyData()
        {
            final XList<XEnum<ByteBuffer>> data = X.List();
            XEnum<ByteBuffer> next;
            while ((next = this.cachedData.poll()) != null)
            {
                data.add(next);
            }

            LOG.trace("Updating object graph");
            this.objectGraphUpdateHandler.objectGraphUpdateAvailable(() ->
            {
                final ObjectMaterializer materializer = new ObjectMaterializer(this.storage.persistenceManager());

                final BinaryEntityRawDataIterator iterator = BinaryEntityRawDataIterator.New();
                for (final var buffers : data)
                {
                    for (final ByteBuffer buffer : buffers)
                    {
                        final long address = XMemory.getDirectByteBufferAddress(buffer);
                        iterator.iterateEntityRawData(address, address + buffer.limit(), materializer);
                        XMemory.deallocateDirectByteBuffer(buffer);

                    }
                }

                materializer.materialize();
            });
            LOG.trace("Finished");
        }

        @Override
        public synchronized void receiveTypeDictionary(final String typeDictionaryData)
        {
            final PersistenceTypeDictionary remoteTypeDictionary = BinaryPersistence.Foundation()
                .setClassLoaderProvider(this.foundation.getClassLoaderProvider())
                .setFieldEvaluatorPersister(this.foundation.getFieldEvaluatorPersistable())
                .setTypeDictionaryLoader(() -> typeDictionaryData)
                .getTypeDictionaryProvider()
                .provideTypeDictionary();
            final PersistenceTypeDictionary localTypeDictionary = this.storage.persistenceManager().typeDictionary();

            remoteTypeDictionary.iterateAllTypeDefinitions(remoteType ->
            {
                final PersistenceTypeDefinition localType = localTypeDictionary.lookupTypeById(remoteType.typeId());
                if (localType == null)
                {
                    LOG.debug("New type: " + remoteType.typeName());
                    this.foundation.getTypeHandlerManager().ensureTypeHandler(remoteType);

                }
                else if (!PersistenceTypeDescription.equalStructure(localType, remoteType))
                {
                    throw new RuntimeException(localType + " <> " + remoteType);
                }
            });
        }

        @Override
        public void dispose()
        {
            this.executor.shutdown();
            try
            {
                // if any external processes like Kubernetes shuts us down, it will wait for the externally set
                // grace period and then kill the process. But any other case we will await the task orderly like this.
                this.executor.awaitTermination(30, TimeUnit.MINUTES);
            }
            catch (final InterruptedException e)
            {
                throw new NodelibraryException(e);
            }
        }
    }
}
