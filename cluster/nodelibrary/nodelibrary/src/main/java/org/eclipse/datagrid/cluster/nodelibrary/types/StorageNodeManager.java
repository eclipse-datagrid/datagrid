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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NotADistributorException;
import org.eclipse.store.storage.types.StorageController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface StorageNodeManager extends ClusterNodeManager
{
    boolean isDistributor();

    void switchToDistribution();

    boolean finishDistributonSwitch() throws NotADistributorException;

    long getCurrentMicrostreamOffset();

    long getLatestMicrostreamOffset();


    static StorageNodeManager New(
        final ClusterStorageBinaryDataDistributor dataDistributor,
        final StorageChecksIssuer storageChecksIssuer,
        final ClusterStorageBinaryDataClient dataClient,
        final StorageNodeHealthCheck healthCheck,
        final StorageController storageController,
        final StorageDiskSpaceReader storageDiskSpaceReader,
        final KafkaOffsetProvider kafkaOffsetProvider
    )
    {
        return new Default(
            notNull(dataDistributor),
            notNull(storageChecksIssuer),
            notNull(dataClient),
            notNull(healthCheck),
            notNull(storageController),
            notNull(storageDiskSpaceReader),
            notNull(kafkaOffsetProvider)
        );
    }

    final class Default implements StorageNodeManager
    {
        private static final Logger LOG = LoggerFactory.getLogger(StorageNodeManager.class);

        private final ClusterStorageBinaryDataDistributor dataDistributor;
        private final StorageChecksIssuer storageChecksIssuer;
        private final ClusterStorageBinaryDataClient dataClient;
        private final StorageNodeHealthCheck healthCheck;
        private final StorageController storageController;
        private final StorageDiskSpaceReader storageDiskSpaceReader;
        private final KafkaOffsetProvider kafkaOffsetProvider;

        private boolean isDistributor;
        private boolean isSwitchingToDistributor;

        public Default(
            final ClusterStorageBinaryDataDistributor dataDistributor,
            final StorageChecksIssuer storageChecksIssuer,
            final ClusterStorageBinaryDataClient dataClient,
            final StorageNodeHealthCheck healthCheck,
            final StorageController storageController,
            final StorageDiskSpaceReader storageDiskSpaceReader,
            final KafkaOffsetProvider kafkaOffsetProvider
        )
        {
            this.dataDistributor = dataDistributor;
            this.dataClient = dataClient;
            this.healthCheck = healthCheck;
            this.storageController = storageController;
            this.storageDiskSpaceReader = storageDiskSpaceReader;
            this.storageChecksIssuer = storageChecksIssuer;
            this.kafkaOffsetProvider = kafkaOffsetProvider;
        }

        @Override
        public void startStorageChecks()
        {
            this.storageChecksIssuer.startChecks();
        }

        @Override
        public boolean isRunningStorageChecks()
        {
            return this.storageChecksIssuer.checksInProgress();
        }

        @Override
        public boolean isReady() throws NodelibraryException
        {
            if (this.isDistributor)
            {
                return this.storageController.isRunning() && !this.storageController.isStartingUp();
            }
            else
            {
                return this.healthCheck.isReady();
            }
        }

        @Override
        public boolean isHealthy()
        {
            if (this.isDistributor)
            {
                return this.storageController.isRunning() && !this.storageController.isStartingUp();
            }
            else
            {
                return this.healthCheck.isHealthy();
            }
        }

        @Override
        public long readStorageSizeBytes() throws NodelibraryException
        {
            return this.storageDiskSpaceReader.readUsedDiskSpaceBytes();
        }

        @Override
        public boolean isDistributor()
        {
            return this.isDistributor;
        }

        @Override
        public void switchToDistribution()
        {
            if (this.isDistributor() || this.isSwitchingToDistributor)
            {
                return;
            }

            LOG.info("Turning on distribution.");
            this.isSwitchingToDistributor = true;
            this.dataClient.stopAtLatestOffset();
        }

        @Override
        public boolean finishDistributonSwitch() throws NotADistributorException
        {
            if (!this.isSwitchingToDistributor)
            {
                throw new NotADistributorException("switchToDistribution() has to be called first");
            }

            if (this.isDistributor)
            {
                return true;
            }

            if (this.dataClient.isRunning())
            {
                return false;
            }

            final var offsetInfo = this.dataClient.offsetInfo();

            // once a node has been switched to distribution it will never become a reader node anymore
            this.healthCheck.close();
            this.dataClient.dispose();
            this.dataDistributor.offset(offsetInfo.msOffset());

            this.isDistributor = true;
            this.isSwitchingToDistributor = false;
            return true;
        }

        @Override
        public long getCurrentMicrostreamOffset()
        {
            if (this.isDistributor())
            {
                return this.dataDistributor.offset();
            }
            else
            {
                return this.dataClient.offsetInfo().msOffset();
            }
        }

        @Override
        public long getLatestMicrostreamOffset()
        {
            return this.kafkaOffsetProvider.provideLatestOffset();
        }

        @Override
        public void close()
        {
            this.dataDistributor.dispose();
            this.dataClient.dispose();
            this.healthCheck.close();
        }
    }
}
