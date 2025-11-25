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

public interface StorageBackupIssuer
{
    void startBackup();

    boolean backupInProgress();

    static StorageBackupIssuer New(final StorageBackupManager backupManager)
    {
        return new Default(notNull(backupManager));
    }

    @FunctionalInterface
    interface Creator
    {
        StorageBackupIssuer create(StorageConnection connection);
    }

    class Default implements StorageBackupIssuer
    {
        private static final Logger LOG = LoggerFactory.getLogger(StorageBackupIssuer.class);
        private final StorageBackupManager backupManager;

        private Thread runner;

        private Default(final StorageBackupManager backupManager)
        {
            this.backupManager = backupManager;
        }

        @Override
        public void startBackup()
        {
            if (this.runner == null || !this.runner.isAlive())
            {
                LOG.debug("Issuing new storage backup");
                this.runner = new Thread(this.backupManager::createStorageBackup, "EclipseStore-StorageBackup-Issuer");
                this.runner.start();
            }
        }

        @Override
        public boolean backupInProgress()
        {
            if (this.runner != null && !this.runner.isAlive())
            {
                LOG.trace("Cleanup previous storage backup thread");
                this.runner = null;
            }
            return this.runner != null;
        }
    }
}
