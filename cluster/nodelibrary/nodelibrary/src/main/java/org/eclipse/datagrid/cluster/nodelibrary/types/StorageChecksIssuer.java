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

import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface StorageChecksIssuer
{
    void startChecks();

    boolean checksInProgress();

    static StorageChecksIssuer New(final StorageConnection storageConnection)
    {
        return new Default(notNull(storageConnection));
    }

    @FunctionalInterface
    interface Creator
    {
        StorageChecksIssuer create(StorageConnection connection);
    }

    class Default implements StorageChecksIssuer
    {
        private static final Logger LOG = LoggerFactory.getLogger(StorageChecksIssuer.class);
        private final StorageConnection connection;

        private Thread runner;

        private Default(final StorageConnection connection)
        {
            this.connection = connection;
        }

        @Override
        public void startChecks()
        {
            if (this.runner == null || !this.runner.isAlive())
            {
                LOG.debug("Issuing new storage checks");
                this.runner = new Thread(() ->
                {
                    this.connection.issueFullGarbageCollection();
                    this.connection.issueFullCacheCheck();
                    this.connection.issueFullFileCheck();
                }, "EclipseStore-StorageChecks-Issuer");
                this.runner.start();
            }
        }

        @Override
        public boolean checksInProgress()
        {
            if (this.runner != null && !this.runner.isAlive())
            {
                LOG.trace("Cleanup previous storage checks thread");
                this.runner = null;
            }
            return this.runner != null;
        }
    }
}
