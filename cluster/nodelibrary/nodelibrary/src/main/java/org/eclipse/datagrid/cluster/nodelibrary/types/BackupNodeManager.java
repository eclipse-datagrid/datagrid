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

import java.io.InputStream;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.meta.NotImplementedYetError;
import org.eclipse.store.storage.types.StorageController;


public interface BackupNodeManager extends ClusterNodeManager
{
    void stopReadingAtLatestOffset();

    void resumeReading() throws NodelibraryException;

    boolean isReading();

    void createStorageBackup() throws NodelibraryException;

    boolean isBackupRunning();

    static BackupNodeManager New(
        final StorageChecksIssuer storageChecksIssuer,
        final StorageBackupIssuer storageBackupIssuer,
        final ClusterStorageBinaryDataClient dataClient,
        final StorageBackupManager backupManager,
        final StorageController storageController,
        final StorageDiskSpaceReader storageDiskSpaceReader
    )
    {
        return new Default(
            notNull(storageChecksIssuer),
            notNull(storageBackupIssuer),
            notNull(dataClient),
            notNull(backupManager),
            notNull(storageController),
            notNull(storageDiskSpaceReader)
        );
    }

    final class Default implements BackupNodeManager
    {
        private final StorageChecksIssuer storageChecksIssuer;
        private final StorageBackupIssuer storageBackupIssuer;
        private final ClusterStorageBinaryDataClient dataClient;
        private final StorageBackupManager backupManager;
        private final StorageController storageController;
        private final StorageDiskSpaceReader storageDiskSpaceReader;

        private Default(
            final StorageChecksIssuer storageChecksIssuer,
            final StorageBackupIssuer storageBackupIssuer,
            final ClusterStorageBinaryDataClient dataClient,
            final StorageBackupManager backupManager,
            final StorageController storageController,
            final StorageDiskSpaceReader storageDiskSpaceReader
        )
        {
            this.storageChecksIssuer = storageChecksIssuer;
            this.storageBackupIssuer = storageBackupIssuer;
            this.dataClient = dataClient;
            this.backupManager = backupManager;
            this.storageController = storageController;
            this.storageDiskSpaceReader = storageDiskSpaceReader;
        }

        @Override
        public void stopReadingAtLatestOffset()
        {
            this.dataClient.stopAtLatestOffset();
        }

        @Override
        public void resumeReading() throws NodelibraryException
        {
            this.dataClient.resume();
        }

        @Override
        public boolean isReading()
        {
            return this.dataClient.isRunning();
        }

        @Override
        public void createStorageBackup() throws NodelibraryException
        {
            this.storageBackupIssuer.startBackup();
        }

        @Override
        public boolean isBackupRunning()
        {
            return this.storageBackupIssuer.backupInProgress();
        }

        @Override
        public boolean isHealthy()
        {
            return this.storageController.isRunning() && !this.storageController.isStartingUp();
        }

        @Override
        public boolean isReady() throws NodelibraryException
        {
            return this.storageController.isRunning() && !this.storageController.isStartingUp();
        }

        @Override
        public boolean isRunningStorageChecks()
        {
            return this.storageChecksIssuer.checksInProgress();
        }

        @Override
        public long readStorageSizeBytes() throws NodelibraryException
        {
            return this.storageDiskSpaceReader.readUsedDiskSpaceBytes();
        }

        @Override
        public void startStorageChecks()
        {
            this.storageChecksIssuer.startChecks();
        }

        @Override
        public void close()
        {
            this.dataClient.dispose();
            this.backupManager.close();
        }
    }
}
