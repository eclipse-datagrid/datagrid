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
import java.util.function.Supplier;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.BadRequestException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.InternalServerErrorException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface ClusterRestRequestController extends AutoCloseable
{
    boolean getMicrostreamDistributor() throws HttpResponseException;

    void postMicrostreamActivateDistributorStart() throws HttpResponseException;

    boolean postMicrostreamActivateDistributorFinish() throws HttpResponseException;

    void getMicrostreamHealth() throws HttpResponseException;

    void getMicrostreamHealthReady() throws HttpResponseException;

    // TODO: Rename to get statistics or monitoring etc.
    String getMicrostreamStorageBytes() throws HttpResponseException;

    void postMicrostreamBackup() throws HttpResponseException;

    boolean getMicrostreamBackup() throws HttpResponseException;

    void postMicrostreamUpdates() throws HttpResponseException;

    boolean getMicrostreamUpdates() throws HttpResponseException;

    void postMicrostreamResumeUpdates() throws HttpResponseException;

    void postMicrostreamGc() throws HttpResponseException;

    boolean getMicrostreamGc() throws HttpResponseException;

    @Override
    void close();

    static ClusterRestRequestController StorageNode(
        final StorageNodeManager storageNodeManager,
        final NodelibraryPropertiesProvider properties
    )
    {
        return new StorageNode(notNull(storageNodeManager), notNull(properties));
    }

    static ClusterRestRequestController DevNode()
    {
        return new DevNode();
    }

    static ClusterRestRequestController MicroNode(
        final MicroNodeManager microNodeManager,
        final NodelibraryPropertiesProvider properties
    )
    {
        return new MicroNode(notNull(microNodeManager), notNull(properties));
    }

    static ClusterRestRequestController BackupNode(
        final BackupNodeManager backupNodeManager,
        final NodelibraryPropertiesProvider properties
    )
    {
        return new BackupNode(notNull(backupNodeManager), notNull(properties));
    }

    abstract class Abstract implements ClusterRestRequestController
    {
        private static final Logger LOG = LoggerFactory.getLogger(ClusterRestRequestController.class);

        private final ClusterNodeManager nodeManager;
        private final NodelibraryPropertiesProvider properties;

        protected Abstract(final ClusterNodeManager nodeManager, final NodelibraryPropertiesProvider properties)
        {
            this.nodeManager = nodeManager;
            this.properties = properties;
        }

        @Override
        public void postMicrostreamGc() throws HttpResponseException
        {
            LOG.trace("Handling postMicrostreamGc request");
            this.handleRequest(this.nodeManager::startStorageChecks);
        }

        @Override
        public boolean getMicrostreamGc() throws HttpResponseException
        {
            LOG.trace("Handling getMicrostreamGc request");
            return this.handleRequest(this.nodeManager::isRunningStorageChecks);
        }

        @Override
        public void getMicrostreamHealth() throws HttpResponseException
        {
            this.handleRequest(() ->
            {
                if (!this.nodeManager.isHealthy())
                {
                    throw new InternalServerErrorException();
                }
            });
        }

        @Override
        public void getMicrostreamHealthReady() throws HttpResponseException
        {
            this.handleRequest(() ->
            {
                if (!this.nodeManager.isReady())
                {
                    throw new InternalServerErrorException();
                }
            });
        }

        @Override
        public String getMicrostreamStorageBytes() throws HttpResponseException
        {
            return this.handleRequest(() ->
            {
                final long storageSizeBytes = this.nodeManager.readStorageSizeBytes();
                return String.format(
                    """
                        # HELP cluster_storage_used_bytes How many bytes are currently used up by the storage.
                        # TYPE cluster_storage_used_bytes gauge
                        cluster_storage_used_bytes{namespace="%s",pod="%s"} %s""",
                    this.properties.myNamespace(),
                    this.properties.myPodName(),
                    storageSizeBytes
                );
            });
        }

        @Override
        public boolean postMicrostreamActivateDistributorFinish() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getMicrostreamDistributor() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getMicrostreamUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postMicrostreamResumeUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postMicrostreamActivateDistributorStart() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postMicrostreamBackup() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getMicrostreamBackup() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postMicrostreamUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        protected void handleRequest(final Runnable request) throws HttpResponseException
        {
            try
            {
                request.run();
            }
            catch (final Exception e)
            {
                // the exception has already been handled
                if (e instanceof HttpResponseException)
                {
                    throw e;
                }

                LOG.error("Failed to handle request", e);
                throw new InternalServerErrorException();
            }
        }

        protected <T> T handleRequest(final Supplier<T> request) throws HttpResponseException
        {
            try
            {
                return request.get();
            }
            catch (final Exception e)
            {
                // the exception has already been handled
                if (e instanceof HttpResponseException)
                {
                    throw e;
                }

                LOG.error("Failed to handle request", e);
                throw new InternalServerErrorException();
            }
        }
    }

    final class StorageNode extends Abstract
    {
        private static final Logger LOG = LoggerFactory.getLogger(StorageNode.class);
        private final StorageNodeManager storageNodeManager;

        private StorageNode(final StorageNodeManager storageNodeManager, final NodelibraryPropertiesProvider properties)
        {
            super(storageNodeManager, properties);
            this.storageNodeManager = storageNodeManager;
        }

        @Override
        public boolean postMicrostreamActivateDistributorFinish() throws HttpResponseException
        {
            return this.handleRequest(this.storageNodeManager::finishDistributonSwitch);
        }

        @Override
        public boolean getMicrostreamDistributor() throws HttpResponseException
        {
            return this.handleRequest(this.storageNodeManager::isDistributor);
        }

        @Override
        public void postMicrostreamActivateDistributorStart() throws HttpResponseException
        {
            LOG.trace("Handling postMicrostreamActivateDistributorStart request");
            this.handleRequest(() ->
            {
                if (!this.storageNodeManager.isDistributor())
                {
                    this.storageNodeManager.switchToDistribution();
                }
            });
        }

        @Override
        public void close()
        {
            this.storageNodeManager.close();
        }
    }

    final class BackupNode extends Abstract
    {
        private static final Logger LOG = LoggerFactory.getLogger(BackupNode.class);
        private final BackupNodeManager backupNodeManager;

        private BackupNode(final BackupNodeManager backupNodeManager, final NodelibraryPropertiesProvider properties)
        {
            super(backupNodeManager, properties);
            this.backupNodeManager = backupNodeManager;
        }

        @Override
        public void postMicrostreamBackup() throws HttpResponseException
        {
            LOG.trace("Handling postMicrostreamBackup request");
            this.handleRequest(this.backupNodeManager::createStorageBackup);

        }

        @Override
        public boolean getMicrostreamBackup() throws HttpResponseException
        {
            return this.handleRequest(this.backupNodeManager::isBackupRunning);

        }

        @Override
        public void postMicrostreamUpdates() throws HttpResponseException
        {
            LOG.trace("Handling postMicrostreamUpdates request");
            this.handleRequest(this.backupNodeManager::stopReadingAtLatestOffset);
        }

        @Override
        public boolean getMicrostreamUpdates() throws HttpResponseException
        {
            return this.handleRequest(() ->
            {
                final boolean isReading = this.backupNodeManager.isReading();
                return !isReading;
            });
        }

        @Override
        public void postMicrostreamResumeUpdates() throws HttpResponseException
        {
            LOG.trace("Handling postMicrostreamResumeUpdates request");
            this.handleRequest(this.backupNodeManager::resumeReading);
        }

        @Override
        public void close()
        {
            this.backupNodeManager.close();

        }
    }

    final class MicroNode extends Abstract
    {
        private static final Logger LOG = LoggerFactory.getLogger(MicroNode.class);
        private final MicroNodeManager microNodeManager;

        private MicroNode(final MicroNodeManager microNodeManager, final NodelibraryPropertiesProvider properties)
        {
            super(microNodeManager, properties);
            this.microNodeManager = microNodeManager;
        }

        @Override
        public boolean postMicrostreamActivateDistributorFinish() throws HttpResponseException
        {
            // micro nodes are always the distributor
            return true;
        }

        @Override
        public boolean getMicrostreamDistributor() throws HttpResponseException
        {
            // micro nodes are always the distributor
            return true;
        }

        @Override
        public void postMicrostreamActivateDistributorStart() throws HttpResponseException
        {
            // micro nodes are always the distributor
        }

        @Override
        public void postMicrostreamBackup() throws HttpResponseException
        {
            LOG.trace("Handling postMicrostreamBackup request");
            this.handleRequest(this.microNodeManager::createStorageBackup);
        }

        @Override
        public boolean getMicrostreamBackup() throws HttpResponseException
        {
            return this.handleRequest(this.microNodeManager::isBackupRunning);
        }

        @Override
        public void close()
        {
            this.microNodeManager.close();
        }
    }

    /**
     * Dev Nodes are just dummy implementations so that the nodelibrary can be
     * tested and run in local development environments.
     */
    final class DevNode implements ClusterRestRequestController
    {
        private DevNode()
        {
        }

        @Override
        public boolean getMicrostreamDistributor() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postMicrostreamActivateDistributorStart() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean postMicrostreamActivateDistributorFinish() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void getMicrostreamHealth() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void getMicrostreamHealthReady() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public String getMicrostreamStorageBytes() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postMicrostreamBackup() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getMicrostreamBackup() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postMicrostreamUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getMicrostreamUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postMicrostreamResumeUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postMicrostreamGc() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getMicrostreamGc() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void close()
        {
            // no-op
        }
    }
}
