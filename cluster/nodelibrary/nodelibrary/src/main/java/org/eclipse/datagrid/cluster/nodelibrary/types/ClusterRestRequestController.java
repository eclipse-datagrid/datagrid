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
    boolean getDataGridDistributor() throws HttpResponseException;

    void postDataGridActivateDistributorStart() throws HttpResponseException;

    boolean postDataGridActivateDistributorFinish() throws HttpResponseException;

    void getDataGridHealth() throws HttpResponseException;

    void getDataGridHealthReady() throws HttpResponseException;

    // TODO: Rename to get statistics or monitoring etc.
    String getDataGridStorageBytes() throws HttpResponseException;

    void postDataGridBackup(PostBackup.Body body) throws HttpResponseException;

    boolean getDataGridBackup() throws HttpResponseException;

    void postDataGridUpdates() throws HttpResponseException;

    boolean getDataGridUpdates() throws HttpResponseException;

    void postDataGridResumeUpdates() throws HttpResponseException;

    void postDataGridGc() throws HttpResponseException;

    boolean getDataGridGc() throws HttpResponseException;

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
        public void postDataGridGc() throws HttpResponseException
        {
            LOG.trace("Handling postDataGridGc request");
            this.handleRequest(this.nodeManager::startStorageChecks);
        }

        @Override
        public boolean getDataGridGc() throws HttpResponseException
        {
            LOG.trace("Handling getDataGridGc request");
            return this.handleRequest(this.nodeManager::isRunningStorageChecks);
        }

        @Override
        public void getDataGridHealth() throws HttpResponseException
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
        public void getDataGridHealthReady() throws HttpResponseException
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
        public String getDataGridStorageBytes() throws HttpResponseException
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
        public boolean postDataGridActivateDistributorFinish() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getDataGridDistributor() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getDataGridUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postDataGridResumeUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postDataGridActivateDistributorStart() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postDataGridBackup(PostBackup.Body body) throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getDataGridBackup() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postDataGridUpdates() throws HttpResponseException
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
        public boolean postDataGridActivateDistributorFinish() throws HttpResponseException
        {
            return this.handleRequest(this.storageNodeManager::finishDistributonSwitch);
        }

        @Override
        public boolean getDataGridDistributor() throws HttpResponseException
        {
            return this.handleRequest(this.storageNodeManager::isDistributor);
        }

        @Override
        public void postDataGridActivateDistributorStart() throws HttpResponseException
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
        public void postDataGridBackup(PostBackup.Body body) throws HttpResponseException
        {
            LOG.trace("Handling postDataGridBackup request");
            this.handleRequest(() -> this.backupNodeManager.createStorageBackup(unbox(body.getUseManualSlot())));
        }

        @Override
        public boolean getDataGridBackup() throws HttpResponseException
        {
            return this.handleRequest(this.backupNodeManager::isBackupRunning);

        }

        @Override
        public void postDataGridUpdates() throws HttpResponseException
        {
            LOG.trace("Handling postDataGridUpdates request");
            this.handleRequest(this.backupNodeManager::stopReadingAtLatestOffset);
        }

        @Override
        public boolean getDataGridUpdates() throws HttpResponseException
        {
            return this.handleRequest(() ->
            {
                final boolean isReading = this.backupNodeManager.isReading();
                return !isReading;
            });
        }

        @Override
        public void postDataGridResumeUpdates() throws HttpResponseException
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
        public boolean postDataGridActivateDistributorFinish() throws HttpResponseException
        {
            // micro nodes are always the distributor
            return true;
        }

        @Override
        public boolean getDataGridDistributor() throws HttpResponseException
        {
            // micro nodes are always the distributor
            return true;
        }

        @Override
        public void postDataGridActivateDistributorStart() throws HttpResponseException
        {
            // micro nodes are always the distributor
        }

        @Override
        public void postDataGridBackup(PostBackup.Body body) throws HttpResponseException
        {
            LOG.trace("Handling postDataGridBackup request");
            this.handleRequest(this.microNodeManager::createStorageBackup);
        }

        @Override
        public boolean getDataGridBackup() throws HttpResponseException
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
        public boolean getDataGridDistributor() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postDataGridActivateDistributorStart() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean postDataGridActivateDistributorFinish() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void getDataGridHealth() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void getDataGridHealthReady() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public String getDataGridStorageBytes() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postDataGridBackup(PostBackup.Body body) throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getDataGridBackup() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postDataGridUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getDataGridUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postDataGridResumeUpdates() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public void postDataGridGc() throws HttpResponseException
        {
            throw new BadRequestException();
        }

        @Override
        public boolean getDataGridGc() throws HttpResponseException
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
