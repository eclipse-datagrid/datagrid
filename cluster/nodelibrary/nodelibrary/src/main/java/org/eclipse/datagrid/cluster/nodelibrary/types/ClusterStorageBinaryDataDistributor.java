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

import static org.eclipse.serializer.util.X.notNull;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.serializer.persistence.binary.types.Binary;


public interface ClusterStorageBinaryDataDistributor extends StorageBinaryDataDistributor
{
    void offset(long offset);

    static ClusterStorageBinaryDataDistributor Caching(final ClusterStorageBinaryDataDistributor delegate)
    {
        return new Caching(notNull(delegate));
    }

    public static final class Caching implements ClusterStorageBinaryDataDistributor
    {
        private final ClusterStorageBinaryDataDistributor delegate;
        private String typeDictionaryData;

        private Caching(final ClusterStorageBinaryDataDistributor delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public synchronized void distributeData(final Binary data)
        {
            if (this.typeDictionaryData != null)
            {
                this.delegate.distributeTypeDictionary(this.typeDictionaryData);
                this.typeDictionaryData = null;
            }
            this.delegate.distributeData(data);
        }

        @Override
        public synchronized void distributeTypeDictionary(final String typeDictionaryData)
        {
            this.typeDictionaryData = typeDictionaryData;
        }

        @Override
        public void offset(final long offset)
        {
            this.delegate.offset(offset);
        }

        @Override
        public void dispose()
        {
            this.delegate.dispose();
        }
    }
}
