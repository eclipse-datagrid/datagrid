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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.BadRequestException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.InternalServerErrorException;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.PostBackup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static org.eclipse.serializer.util.X.notNull;
import static org.eclipse.serializer.util.X.unbox;


public interface ClusterRestRequestController extends AutoCloseable
{
    boolean getDistributor() throws HttpResponseException;

    void postActivateDistributorStart() throws HttpResponseException;

    boolean postActivateDistributorFinish() throws HttpResponseException;

    void getHealth() throws HttpResponseException;

    void getHealthReady() throws HttpResponseException;

    // TODO: Rename to get statistics or monitoring etc.
    String getStorageBytes() throws HttpResponseException;

    void postBackup(PostBackup.Body body) throws HttpResponseException;

    boolean getBackup() throws HttpResponseException;

    void postUpdates() throws HttpResponseException;

    boolean getUpdates() throws HttpResponseException;

    void postResumeUpdates() throws HttpResponseException;

    void postGc() throws HttpResponseException;

    boolean getGc() throws HttpResponseException;

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
        public void postGc() throws HttpResponseException
        {
            LOG.trace("Handling postDataGridGc request");
            this.handleRequest(this.nodeManager::startStorageChecks);
        }

        @Override
        public boolean getGc() throws HttpResponseException
        {
            LOG.trace("Handling getDataGridGc request");
            return this.handleRequest(this.nodeManager::isRunningStorageChecks);
        }

        @Override
        public void getHealth() throws HttpResponseException
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
        public void getHealthReady() throws HttpResponseException
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
        public String getStorageBytes() throws HttpResponseException
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
        public boolean postActivateDistributorFinish() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getDistributor() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postResumeUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postActivateDistributorStart() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postBackup(PostBackup.Body body) throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getBackup() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postUpdates() throws HttpResponseException
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
        public boolean postActivateDistributorFinish() throws HttpResponseException
        {
            return this.handleRequest(this.storageNodeManager::finishDistributonSwitch);
        }

        @Override
        public boolean getDistributor() throws HttpResponseException
        {
            return this.handleRequest(this.storageNodeManager::isDistributor);
        }

        @Override
        public void postActivateDistributorStart() throws HttpResponseException
        {
            LOG.trace("Handling postDataGridActivateDistributorStart request");
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
        public void postBackup(PostBackup.Body body) throws HttpResponseException
        {
            LOG.trace("Handling postDataGridBackup request");
            this.handleRequest(() -> this.backupNodeManager.createStorageBackup(unbox(body.getUseManualSlot())));
        }

        @Override
        public boolean getBackup() throws HttpResponseException
        {
            return this.handleRequest(this.backupNodeManager::isBackupRunning);

        }

        @Override
        public void postUpdates() throws HttpResponseException
        {
            LOG.trace("Handling postDataGridUpdates request");
            this.handleRequest(this.backupNodeManager::stopReadingAtLatestOffset);
        }

        @Override
        public boolean getUpdates() throws HttpResponseException
        {
            return this.handleRequest(() ->
            {
                final boolean isReading = this.backupNodeManager.isReading();
                return !isReading;
            });
        }

        @Override
        public void postResumeUpdates() throws HttpResponseException
        {
            LOG.trace("Handling postDataGridResumeUpdates request");
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
        public boolean postActivateDistributorFinish() throws HttpResponseException
        {
            // micro nodes are always the distributor
            return true;
        }

        @Override
        public boolean getDistributor() throws HttpResponseException
        {
            // micro nodes are always the distributor
            return true;
        }

        @Override
        public void postActivateDistributorStart() throws HttpResponseException
        {
            // micro nodes are always the distributor
        }

        @Override
        public void postBackup(PostBackup.Body body) throws HttpResponseException
        {
            LOG.trace("Handling postDataGridBackup request");
            this.handleRequest(this.microNodeManager::createStorageBackup);
        }

        @Override
        public boolean getBackup() throws HttpResponseException
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
        public boolean getDistributor() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postActivateDistributorStart() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean postActivateDistributorFinish() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void getHealth() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void getHealthReady() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public String getStorageBytes() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postBackup(PostBackup.Body body) throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getBackup() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postResumeUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postGc() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getGc() throws HttpResponseException
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
