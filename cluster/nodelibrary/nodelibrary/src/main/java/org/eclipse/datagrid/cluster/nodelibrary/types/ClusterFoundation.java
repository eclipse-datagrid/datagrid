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

import java.io.File;
import java.nio.file.Paths;
import java.util.function.Supplier;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.storage.distributed.types.DistributedStorage;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMerger;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAcceptor;
import org.eclipse.serializer.exceptions.MissingFoundationPartException;
import org.eclipse.serializer.persistence.types.Unpersistable;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.serializer.util.InstanceDispatcher;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.exceptions.StorageException;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.eclipse.store.storage.types.StorageExceptionHandler;
import org.eclipse.store.storage.types.StorageLiveFileProvider;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface ClusterFoundation<F extends ClusterFoundation<?>> extends InstanceDispatcher
{
    KafkaPropertiesProvider getKafkaPropertiesProvider();

    F setKafkaPropertiesProvider(KafkaPropertiesProvider provider);

    StorageChecksIssuer getStorageChecksIssuer();

    F setStorageChecksIssuer(StorageChecksIssuer issuer);

    StorageBackupIssuer getStorageBackupIssuer();

    F setStorageBackupIssuer(StorageBackupIssuer issuer);

    StorageBinaryDataPacketAcceptor getStorageBinaryDataPacketAcceptor();

    F setStorageBinaryDataPacketAcceptor(StorageBinaryDataPacketAcceptor acceptor);

    StorageBinaryDataMerger getStorageBinaryDataMerger();

    F setStorageBinaryDataMerger(StorageBinaryDataMerger merger);

    AfterDataMessageConsumedListener getAfterDataMessageConsumedListener();

    F setAfterDataMessageConsumedListener(AfterDataMessageConsumedListener listener);

    StoredOffsetManager getStoredOffsetManager();

    F setStoredOffsetManager(StoredOffsetManager manager);

    StorageBackupManager getStorageBackupManager();

    F setStorageBackupManager(StorageBackupManager manager);

    StorageLimitChecker getStorageLimitChecker();

    F setStorageLimitChecker(StorageLimitChecker checker);

    Supplier<Object> getRootSupplier();

    F setRootSupplier(Supplier<Object> supplier);

    ObjectGraphUpdateHandler getObjectGraphUpdateHandler();

    F setObjectGraphUpdateHandler(ObjectGraphUpdateHandler handler);

    EmbeddedStorageFoundation<?> getEmbeddedStorageFoundation();

    F setEmbeddedStorageFoundation(EmbeddedStorageFoundation<?> foundation);

    BackupNodeManager getBackupNodeManager();

    F setBackupNodeManager(BackupNodeManager manager);

    ClusterStorageBinaryDataClient getClusterStorageBinaryDataClient();

    F setClusterStorageBinaryDataClient(ClusterStorageBinaryDataClient client);

    ClusterStorageBinaryDataDistributor getClusterStorageBinaryDataDistributor();

    F setClusterStorageBinaryDataDistributor(ClusterStorageBinaryDataDistributor distributor);

    MicroNodeManager getMicroNodeManager();

    F setMicroNodeManager(MicroNodeManager manager);

    StorageNodeHealthCheck getStorageNodeHealthCheck();

    F setStorageNodeHealthCheck(StorageNodeHealthCheck check);

    NodelibraryPropertiesProvider getNodelibraryPropertiesProvider();

    F setNodelibraryPropertiesProvider(NodelibraryPropertiesProvider provider);

    StorageChecksIssuer.Creator getStorageChecksIssuerCreator();

    F setStorageChecksIssuerCreator(StorageChecksIssuer.Creator creator);

    StorageDiskSpaceReader getStorageDiskSpaceReader();

    F setStorageDiskSpaceReader(StorageDiskSpaceReader reader);

    StorageNodeManager getStorageNodeManager();

    F setStorageNodeManager(StorageNodeManager manager);

    boolean getEnableAsyncDistribution();

    F setEnableAsyncDistribution(boolean enable);

    KafkaOffsetProvider getKafkaOffsetProvider();

    F setKafkaOffsetProvider(KafkaOffsetProvider provider);

    static ClusterFoundation<?> New()
    {
        return new Default<>();
    }

    ClusterRestRequestController startController() throws NodelibraryException;

    ClusterStorageManager<?> startStorageManager() throws NodelibraryException;

    class Default<F extends Default<?>> extends InstanceDispatcher.Default implements
        ClusterFoundation<F>,
        Unpersistable
    {
        private static final Logger LOG = LoggerFactory.getLogger(ClusterFoundation.class);

        private EmbeddedStorageFoundation<?> embeddedStorageFoundation;
        private StorageLimitChecker limitChecker;
        private BackupNodeManager backupNodeManager;
        private ClusterStorageBinaryDataClient dataClient;
        private ClusterStorageBinaryDataDistributor dataDistributor;
        private MicroNodeManager microNodeManager;
        private StorageNodeHealthCheck healthCheck;
        private NodelibraryPropertiesProvider propertiesProvider;
        private StorageChecksIssuer.Creator storageChecksIssuerCreator;
        private StorageChecksIssuer storageChecksIssuer;
        private StorageBackupIssuer storageBackupIssuer;
        private StorageDiskSpaceReader storageDiskSpaceReader;
        private StorageNodeManager storageNodeManager;
        private boolean enableAsyncDistribution;
        private Supplier<Object> rootSupplier;
        private ObjectGraphUpdateHandler graphUpdateHandler;
        private StorageBackupManager storageBackupManager;
        private AfterDataMessageConsumedListener afterDataMessageConsumedListener;
        private StorageBinaryDataMerger dataMerger;
        private StorageBinaryDataPacketAcceptor dataPacketAcceptor;
        private StoredOffsetManager storedOffsetManager;
        private KafkaPropertiesProvider kafkaPropertiesProvider;
        private KafkaOffsetProvider kafkaOffsetProvider;

        // cached created types
        private ClusterStorageManager<?> clusterStorageManager;
        private ClusterRestRequestController clusterRequestController;

        private Default()
        {
        }

        @SuppressWarnings("unchecked")
        protected final F $()
        {
            return (F)this;
        }

        protected KafkaOffsetProvider ensureKafkaOffsetProvider()
        {
            final var nodelibProps = this.getNodelibraryPropertiesProvider();
            final var kafkaProps = this.getKafkaPropertiesProvider();

            final String topic = nodelibProps.kafkaTopicName();
            final String groupId = String.format("%s-%s-offsetgetter", topic, nodelibProps.myPodName());

            return KafkaOffsetProvider.New(topic, groupId, kafkaProps);
        }

        protected KafkaPropertiesProvider ensureKafkaPropertiesProvider()
        {
            return KafkaPropertiesProvider.New(this.getNodelibraryPropertiesProvider());
        }

        protected StoredOffsetManager ensureStoredOffsetManager()
        {
            // TODO: Hardcoded path
            final var offsetPath = Paths.get("/storage/offset");
            LOG.trace("Creating stored offset manager for offset file at {}", offsetPath);
            return StoredOffsetManager.New(NioFileSystem.New().ensureFile(offsetPath).tryUseWriting());
        }

        protected AfterDataMessageConsumedListener ensureAfterDataMessageConsumedListener()
        {
            final var props = this.getNodelibraryPropertiesProvider();
            final var storageChecks = AfterDataMessageConsumedListener.RunStorageChecks(
                notNull(this.clusterStorageManager)
            );
            final var storedOffsetUpdater = new AfterDataMessageConsumedListener()
            {
                final StoredOffsetManager delegate = ClusterFoundation.Default.this.getStoredOffsetManager();

                @Override
                public void onChange(final OffsetInfo offsetInfo) throws NodelibraryException
                {
                    if (props.isBackupNode())
                    {
                        // only backup nodes shall update the stored offset
                        this.delegate.set(offsetInfo);
                    }
                }

                @Override
                public void close()
                {
                    this.delegate.close();
                }
            };
            LOG.trace(
                "Created AfterDataMessageConsumedListener->StoredOffsetManager delegate. WillRun={}",
                props.isBackupNode()
            );
            return AfterDataMessageConsumedListener.Combined(storageChecks, storedOffsetUpdater);
        }

        protected StorageBackupManager ensureStorageBackupManager()
        {
            // TODO: Hardcoded path
            final var backupsDirPath = new File("/backups");
            final int maxBackupCount = this.getNodelibraryPropertiesProvider().keptBackupsCount();
            final Supplier<OffsetInfo> offsetProvider = this.getClusterStorageBinaryDataClient()::offsetInfo;
            final StoredOffsetManager.Creator offsetManagerCreator = StoredOffsetManager::New;
            return StorageBackupManager.New(
                this.clusterStorageManager,
                backupsDirPath,
                maxBackupCount,
                offsetProvider,
                offsetManagerCreator
            );
        }

        protected StorageLimitChecker ensureLimitChecker()
        {
            // TODO: Do this differently
            final var p = this.getNodelibraryPropertiesProvider();
            final int errorGb = (int)Math.round(p.storageLimitGB() / 100.0 * p.storageLimitCheckerPercent());
            final int intervalMins = p.storageLimitCheckerIntervalMinutes();
            final Scheduler s;
            try
            {
                s = StdSchedulerFactory.getDefaultScheduler();
            }
            catch (final SchedulerException e)
            {
                throw new NodelibraryException(e);
            }
            LOG.trace("Created storage limit checker with interval {}min and error at {}gb", intervalMins, errorGb);
            return StorageLimitChecker.New(intervalMins, errorGb, s, this.getStorageDiskSpaceReader());
        }

        protected Supplier<Object> ensureRootSupplier()
        {
            throw new MissingFoundationPartException(Supplier.class, "Missing root supplier");
        }

        protected ObjectGraphUpdateHandler ensureGraphUpdateHandler()
        {
            return ObjectGraphUpdateHandler.Synchronized();
        }

        protected EmbeddedStorageFoundation<?> ensureEmbeddedStorageFoundation()
        {
            return EmbeddedStorageFoundation.New();
        }

        protected StorageBackupIssuer ensureStorageBackupIssuer()
        {
            return StorageBackupIssuer.New(this.getStorageBackupManager());
        }

        protected BackupNodeManager ensureBackupNodeManager()
        {
            return BackupNodeManager.New(
                this.getStorageChecksIssuer(),
                this.getStorageBackupIssuer(),
                this.getClusterStorageBinaryDataClient(),
                this.getStorageBackupManager(),
                this.clusterStorageManager,
                this.getStorageDiskSpaceReader()
            );
        }

        protected ClusterStorageBinaryDataClient ensureClusterStorageBinaryDataClient()
        {
            final var props = this.getNodelibraryPropertiesProvider();
            final var topic = props.kafkaTopicName();
            final String groupId;
            final boolean doCommitOffset;
            if (props.isBackupNode())
            {
                groupId = topic + "-backup";
                doCommitOffset = true;
            }
            else
            {
                final var podName = props.myPodName();
                groupId = topic + "-" + podName;
                doCommitOffset = false;
            }
            LOG.trace("Created data client with group id {}", groupId);
            return ClusterStorageBinaryDataClient.New(
                this.getStorageBinaryDataPacketAcceptor(),
                topic,
                groupId,
                this.getAfterDataMessageConsumedListener(),
                this.getStoredOffsetManager().get(),
                this.getKafkaPropertiesProvider(),
                doCommitOffset
            );
        }

        protected MicroNodeManager ensureMicroNodeManager()
        {
            return MicroNodeManager.New(
                this.getStorageChecksIssuer(),
                this.clusterStorageManager,
                this.getStorageBackupManager(),
                this.getStorageDiskSpaceReader()
            );
        }

        protected StorageNodeHealthCheck ensureStorageNodeHealthCheck()
        {
            final var properties = this.getNodelibraryPropertiesProvider();
            final var topic = properties.kafkaTopicName();
            final var podName = properties.myPodName();
            // TODO: Hardcoded
            final var groupId = String.format("%s-%s-readiness", topic, podName);
            return StorageNodeHealthCheck.New(
                topic,
                groupId,
                this.clusterStorageManager,
                this.getClusterStorageBinaryDataClient(),
                this.getKafkaPropertiesProvider()
            );
        }

        protected NodelibraryPropertiesProvider ensureNodelibraryPropertiesProvider()
        {
            return NodelibraryPropertiesProvider.Env();
        }

        protected StorageChecksIssuer ensureStorageChecksIssuer()
        {
            throw new MissingFoundationPartException(StorageChecksIssuer.class);
        }

        protected StorageChecksIssuer.Creator ensureStorageChecksIssuerCreator()
        {
            return StorageChecksIssuer::New;
        }

        protected StorageDiskSpaceReader ensureStorageDiskSpaceReader()
        {
            return StorageDiskSpaceReader.New(
                this.getEmbeddedStorageFoundation().getConfiguration().fileProvider().baseDirectory()
            );
        }

        protected StorageNodeManager ensureStorageNodeManager()
        {
            return StorageNodeManager.New(
                this.getClusterStorageBinaryDataDistributor(),
                this.getStorageChecksIssuer(),
                this.getClusterStorageBinaryDataClient(),
                this.getStorageNodeHealthCheck(),
                this.clusterStorageManager,
                this.getStorageDiskSpaceReader(),
                this.getKafkaOffsetProvider()
            );
        }

        protected ClusterStorageBinaryDataDistributor ensureDataDistributor()
        {
            final var topic = this.getNodelibraryPropertiesProvider().kafkaTopicName();
            final ClusterStorageBinaryDataDistributor delegate;
            if (this.getEnableAsyncDistribution())
            {
                delegate = ClusterStorageBinaryDataDistributorKafka.Async(topic, this.getKafkaPropertiesProvider());
                LOG.info("Using async kafka data distributor");
            }
            else
            {
                delegate = ClusterStorageBinaryDataDistributorKafka.Sync(topic, this.getKafkaPropertiesProvider());
                LOG.info("Using sync kafka data distributor");
            }
            return ClusterStorageBinaryDataDistributor.Caching(delegate);
        }

        protected StorageBinaryDataMerger ensureStorageBinaryDataMerger()
        {
            return StorageBinaryDataMerger.New(
                this.getEmbeddedStorageFoundation().getConnectionFoundation(),
                this.clusterStorageManager,
                this.getObjectGraphUpdateHandler()
            );
        }

        protected StorageBinaryDataPacketAcceptor ensureDataPacketAcceptor()
        {
            return StorageBinaryDataPacketAcceptor.New(this.getStorageBinaryDataMerger());
        }

        @Override
        public KafkaPropertiesProvider getKafkaPropertiesProvider()
        {
            if (this.kafkaPropertiesProvider == null)
            {
                this.kafkaPropertiesProvider = this.dispatch(this.ensureKafkaPropertiesProvider());
            }
            return this.kafkaPropertiesProvider;
        }

        @Override
        public F setKafkaPropertiesProvider(final KafkaPropertiesProvider provider)
        {
            this.kafkaPropertiesProvider = provider;
            return this.$();
        }

        @Override
        public StorageBackupManager getStorageBackupManager()
        {
            if (this.storageBackupManager == null)
            {
                this.storageBackupManager = this.dispatch(this.ensureStorageBackupManager());
            }
            return this.storageBackupManager;
        }

        @Override
        public F setStorageBackupManager(final StorageBackupManager manager)
        {
            this.storageBackupManager = manager;
            return this.$();
        }

        @Override
        public ObjectGraphUpdateHandler getObjectGraphUpdateHandler()
        {
            if (this.graphUpdateHandler == null)
            {
                this.graphUpdateHandler = this.dispatch(this.ensureGraphUpdateHandler());
            }
            return this.graphUpdateHandler;
        }

        @Override
        public F setObjectGraphUpdateHandler(final ObjectGraphUpdateHandler handler)
        {
            this.graphUpdateHandler = handler;
            return this.$();
        }

        @Override
        public Supplier<Object> getRootSupplier()
        {
            if (this.rootSupplier == null)
            {
                this.rootSupplier = this.dispatch(this.ensureRootSupplier());
            }
            return this.rootSupplier;
        }

        @Override
        public F setRootSupplier(final Supplier<Object> supplier)
        {
            this.rootSupplier = supplier;
            return this.$();
        }

        @Override
        public boolean getEnableAsyncDistribution()
        {
            return this.enableAsyncDistribution;
        }

        @Override
        public F setEnableAsyncDistribution(final boolean enable)
        {
            this.enableAsyncDistribution = enable;
            return this.$();
        }

        @Override
        public EmbeddedStorageFoundation<?> getEmbeddedStorageFoundation()
        {
            if (this.embeddedStorageFoundation == null)
            {
                this.embeddedStorageFoundation = this.dispatch(this.ensureEmbeddedStorageFoundation());
            }
            return this.embeddedStorageFoundation;
        }

        @Override
        public F setEmbeddedStorageFoundation(final EmbeddedStorageFoundation<?> foundation)
        {
            this.embeddedStorageFoundation = foundation;
            return this.$();
        }

        @Override
        public BackupNodeManager getBackupNodeManager()
        {
            if (this.backupNodeManager == null)
            {
                this.backupNodeManager = this.dispatch(this.ensureBackupNodeManager());
            }
            return this.backupNodeManager;
        }

        @Override
        public F setBackupNodeManager(final BackupNodeManager manager)
        {
            this.backupNodeManager = manager;
            return this.$();
        }

        @Override
        public ClusterStorageBinaryDataClient getClusterStorageBinaryDataClient()
        {
            if (this.dataClient == null)
            {
                this.dataClient = this.dispatch(this.ensureClusterStorageBinaryDataClient());
            }
            return this.dataClient;
        }

        @Override
        public F setClusterStorageBinaryDataClient(final ClusterStorageBinaryDataClient client)
        {
            this.dataClient = client;
            return this.$();
        }

        @Override
        public ClusterStorageBinaryDataDistributor getClusterStorageBinaryDataDistributor()
        {
            if (this.dataDistributor == null)
            {
                this.dataDistributor = this.dispatch(this.ensureDataDistributor());
            }
            return this.dataDistributor;
        }

        @Override
        public F setClusterStorageBinaryDataDistributor(final ClusterStorageBinaryDataDistributor distributor)
        {
            this.dataDistributor = distributor;
            return this.$();
        }

        @Override
        public MicroNodeManager getMicroNodeManager()
        {
            if (this.microNodeManager == null)
            {
                this.microNodeManager = this.dispatch(this.ensureMicroNodeManager());
            }
            return this.microNodeManager;
        }

        @Override
        public F setMicroNodeManager(final MicroNodeManager manager)
        {
            this.microNodeManager = manager;
            return this.$();
        }

        @Override
        public StorageNodeHealthCheck getStorageNodeHealthCheck()
        {
            if (this.healthCheck == null)
            {
                this.healthCheck = this.dispatch(this.ensureStorageNodeHealthCheck());
            }
            return this.healthCheck;
        }

        @Override
        public F setStorageNodeHealthCheck(final StorageNodeHealthCheck check)
        {
            this.healthCheck = check;
            return this.$();
        }

        @Override
        public NodelibraryPropertiesProvider getNodelibraryPropertiesProvider()
        {
            if (this.propertiesProvider == null)
            {
                this.propertiesProvider = this.dispatch(this.ensureNodelibraryPropertiesProvider());
            }
            return this.propertiesProvider;
        }

        @Override
        public F setNodelibraryPropertiesProvider(final NodelibraryPropertiesProvider provider)
        {
            this.propertiesProvider = provider;
            return this.$();
        }

        @Override
        public StorageDiskSpaceReader getStorageDiskSpaceReader()
        {
            if (this.storageDiskSpaceReader == null)
            {
                this.storageDiskSpaceReader = this.dispatch(this.ensureStorageDiskSpaceReader());
            }
            return this.storageDiskSpaceReader;
        }

        @Override
        public F setStorageDiskSpaceReader(final StorageDiskSpaceReader reader)
        {
            this.storageDiskSpaceReader = reader;
            return this.$();
        }

        @Override
        public StorageNodeManager getStorageNodeManager()
        {
            if (this.storageNodeManager == null)
            {
                this.storageNodeManager = this.dispatch(this.ensureStorageNodeManager());
            }
            return this.storageNodeManager;
        }

        @Override
        public F setStorageNodeManager(final StorageNodeManager manager)
        {
            this.storageNodeManager = manager;
            return this.$();
        }

        @Override
        public AfterDataMessageConsumedListener getAfterDataMessageConsumedListener()
        {
            if (this.afterDataMessageConsumedListener == null)
            {
                this.afterDataMessageConsumedListener = this.dispatch(this.ensureAfterDataMessageConsumedListener());
            }
            return this.afterDataMessageConsumedListener;
        }

        @Override
        public F setAfterDataMessageConsumedListener(final AfterDataMessageConsumedListener listener)
        {
            this.afterDataMessageConsumedListener = listener;
            return this.$();
        }

        @Override
        public StorageChecksIssuer getStorageChecksIssuer()
        {
            if (this.storageChecksIssuer == null)
            {
                this.storageChecksIssuer = this.getStorageChecksIssuerCreator()
                    .create(notNull(this.clusterStorageManager));
            }
            return this.storageChecksIssuer;
        }

        @Override
        public F setStorageChecksIssuer(final StorageChecksIssuer issuer)
        {
            this.storageChecksIssuer = issuer;
            return this.$();
        }

        @Override
        public StorageBackupIssuer getStorageBackupIssuer()
        {
            if (this.storageBackupIssuer == null)
            {
                this.storageBackupIssuer = this.dispatch(this.ensureStorageBackupIssuer());
            }
            return this.storageBackupIssuer;
        }

        @Override
        public F setStorageBackupIssuer(final StorageBackupIssuer issuer)
        {
            this.storageBackupIssuer = issuer;
            return this.$();
        }

        @Override
        public StorageBinaryDataMerger getStorageBinaryDataMerger()
        {
            if (this.dataMerger == null)
            {
                this.dataMerger = this.dispatch(this.ensureStorageBinaryDataMerger());
            }
            return this.dataMerger;
        }

        @Override
        public F setStorageBinaryDataMerger(final StorageBinaryDataMerger merger)
        {
            this.dataMerger = merger;
            return this.$();
        }

        @Override
        public StorageBinaryDataPacketAcceptor getStorageBinaryDataPacketAcceptor()
        {
            if (this.dataPacketAcceptor == null)
            {
                this.dataPacketAcceptor = this.dispatch(this.ensureDataPacketAcceptor());
            }
            return this.dataPacketAcceptor;
        }

        @Override
        public F setStorageBinaryDataPacketAcceptor(final StorageBinaryDataPacketAcceptor acceptor)
        {
            this.dataPacketAcceptor = acceptor;
            return this.$();
        }

        @Override
        public StorageChecksIssuer.Creator getStorageChecksIssuerCreator()
        {
            if (this.storageChecksIssuerCreator == null)
            {
                this.storageChecksIssuerCreator = this.dispatch(this.ensureStorageChecksIssuerCreator());
            }
            return this.storageChecksIssuerCreator;
        }

        @Override
        public F setStorageChecksIssuerCreator(final StorageChecksIssuer.Creator creator)
        {
            this.storageChecksIssuerCreator = creator;
            return this.$();
        }

        @Override
        public StorageLimitChecker getStorageLimitChecker()
        {
            if (this.limitChecker == null)
            {
                this.limitChecker = this.dispatch(this.ensureLimitChecker());
            }
            return this.limitChecker;
        }

        @Override
        public F setStorageLimitChecker(final StorageLimitChecker checker)
        {
            this.limitChecker = checker;
            return this.$();
        }

        @Override
        public StoredOffsetManager getStoredOffsetManager()
        {
            if (this.storedOffsetManager == null)
            {
                this.storedOffsetManager = this.dispatch(this.ensureStoredOffsetManager());
            }
            return this.storedOffsetManager;
        }

        @Override
        public F setStoredOffsetManager(final StoredOffsetManager manager)
        {
            this.storedOffsetManager = manager;
            return this.$();
        }

        @Override
        public KafkaOffsetProvider getKafkaOffsetProvider()
        {
            if (this.kafkaOffsetProvider == null)
            {
                this.kafkaOffsetProvider = this.dispatch(this.ensureKafkaOffsetProvider());
            }
            return this.kafkaOffsetProvider;
        }

        @Override
        public F setKafkaOffsetProvider(final KafkaOffsetProvider provider)
        {
            this.kafkaOffsetProvider = provider;
            return this.$();
        }

        @Override
        public ClusterRestRequestController startController() throws NodelibraryException
        {
            if (this.clusterRequestController == null)
            {
                this.start();
            }

            return this.clusterRequestController;
        }

        @Override
        public ClusterStorageManager<?> startStorageManager() throws NodelibraryException
        {
            if (this.clusterStorageManager == null)
            {
                this.start();
            }

            return this.clusterStorageManager;
        }

        protected void start() throws NodelibraryException
        {
            final var properties = this.getNodelibraryPropertiesProvider();

            if (!properties.isProdMode())
            {
                this.startDevNode();
            }
            else if (properties.isMicro())
            {
                this.startMicroNode();
            }
            else if (properties.isBackupNode())
            {
                this.startBackupNode();
            }
            else
            {
                this.startStorageNode();
            }
        }

        protected void startBackupNode() throws NodelibraryException
        {
            LOG.info("Starting backup cluster node");

            this.getKafkaOffsetProvider().init();

            final var logger = LoggerFactory.getLogger(ClusterFoundation.class);
            // TODO: Hardcoded paths
            final var storagePath = Paths.get("/storage/storage");

            logger.info("Creating nodelibrary cluster controller");

            final var embeddedStorageFoundation = this.getEmbeddedStorageFoundation();
            // replace the storage live file provider from the provided embedded storage foundation
            StorageConfiguration storageConfig = embeddedStorageFoundation.getConfiguration();
            storageConfig = StorageConfiguration.Builder()
                .setBackupSetup(storageConfig.backupSetup())
                .setChannelCountProvider(storageConfig.channelCountProvider())
                .setDataFileEvaluator(storageConfig.dataFileEvaluator())
                .setEntityCacheEvaluator(storageConfig.entityCacheEvaluator())
                .setHousekeepingController(storageConfig.housekeepingController())
                .setStorageFileProvider(StorageLiveFileProvider.New(NioFileSystem.New().ensureDirectory(storagePath)))
                .createConfiguration();
            embeddedStorageFoundation.setConfiguration(storageConfig);

            embeddedStorageFoundation.setExceptionHandler((throwable, channel) ->
            {
                try
                {
                    StorageExceptionHandler.defaultHandleException(throwable, channel);
                }
                catch (final StorageException exception)
                {
                    GlobalErrorHandling.handleFatalError(exception);
                }
            });

            final var embeddedStorageManager = embeddedStorageFoundation.start();

            if (embeddedStorageManager.root() == null)
            {
                final var root = this.getRootSupplier().get();
                if (root instanceof Lazy)
                {
                    embeddedStorageManager.setRoot(root);
                }
                else
                {
                    embeddedStorageManager.setRoot(Lazy.Reference(root));
                }
                embeddedStorageManager.storeRoot();
            }

            this.clusterStorageManager = ClusterStorageManager.Wrapper(embeddedStorageManager);

            this.getClusterStorageBinaryDataClient().start();

            this.clusterRequestController = ClusterRestRequestController.BackupNode(
                this.getBackupNodeManager(),
                this.getNodelibraryPropertiesProvider()
            );
        }

        protected void startMicroNode() throws NodelibraryException
        {
            LOG.info("Starting micro cluster node");

            // TODO: Hardcoded paths
            final var storagePath = Paths.get("/storage/storage");
            final EmbeddedStorageFoundation<?> embeddedStorageFoundation = this.getEmbeddedStorageFoundation();

            // replace the storage live file provider from the provided embedded storage foundation
            StorageConfiguration storageConfig = embeddedStorageFoundation.getConfiguration();
            storageConfig = StorageConfiguration.Builder()
                .setBackupSetup(storageConfig.backupSetup())
                .setChannelCountProvider(storageConfig.channelCountProvider())
                .setDataFileEvaluator(storageConfig.dataFileEvaluator())
                .setEntityCacheEvaluator(storageConfig.entityCacheEvaluator())
                .setHousekeepingController(storageConfig.housekeepingController())
                .setStorageFileProvider(StorageLiveFileProvider.New(NioFileSystem.New().ensureDirectory(storagePath)))
                .createConfiguration();
            embeddedStorageFoundation.setConfiguration(storageConfig);

            embeddedStorageFoundation.setExceptionHandler((throwable, channel) ->
            {
                try
                {
                    StorageExceptionHandler.defaultHandleException(throwable, channel);
                }
                catch (final StorageException exception)
                {
                    GlobalErrorHandling.handleFatalError(exception);
                }
            });

            final var embeddedStorageManager = embeddedStorageFoundation.start();

            if (embeddedStorageManager.root() == null)
            {
                final var root = this.rootSupplier.get();
                if (root instanceof Lazy)
                {
                    embeddedStorageManager.setRoot(root);
                }
                else
                {
                    embeddedStorageManager.setRoot(Lazy.Reference(root));
                }
                embeddedStorageManager.storeRoot();
            }

            final StorageLimitChecker storageLimitChecker = this.getStorageLimitChecker();
            storageLimitChecker.start();

            this.clusterStorageManager = ClusterStorageManager.New(embeddedStorageManager, storageLimitChecker);
            this.clusterRequestController = ClusterRestRequestController.MicroNode(
                this.getMicroNodeManager(),
                this.getNodelibraryPropertiesProvider()
            );
        }

        protected void startDevNode() throws NodelibraryException
        {
            LOG.info("Starting dev cluster node");
            final var storage = this.getEmbeddedStorageFoundation().start();
            if (storage.root() == null)
            {
                final var root = this.getRootSupplier().get();
                if (root instanceof Lazy)
                {
                    storage.setRoot(root);
                }
                else
                {
                    storage.setRoot(Lazy.Reference(root));
                }
                storage.storeRoot();
            }

            this.clusterStorageManager = ClusterStorageManager.Wrapper(storage);
            this.clusterRequestController = ClusterRestRequestController.DevNode();
        }

        protected void startStorageNode() throws NodelibraryException
        {
            LOG.info("Starting storage cluster node");

            this.getKafkaOffsetProvider().init();

            // TODO: Hardcoded paths
            final var storagePath = Paths.get("/storage/storage");

            final var embeddedStorageFoundation = this.getEmbeddedStorageFoundation();
            // replace the storage live file provider from the provided embedded storage foundation
            StorageConfiguration storageConfig = embeddedStorageFoundation.getConfiguration();
            storageConfig = StorageConfiguration.Builder()
                .setBackupSetup(storageConfig.backupSetup())
                .setChannelCountProvider(storageConfig.channelCountProvider())
                .setDataFileEvaluator(storageConfig.dataFileEvaluator())
                .setEntityCacheEvaluator(storageConfig.entityCacheEvaluator())
                .setHousekeepingController(storageConfig.housekeepingController())
                .setStorageFileProvider(StorageLiveFileProvider.New(NioFileSystem.New().ensureDirectory(storagePath)))
                .createConfiguration();
            embeddedStorageFoundation.setConfiguration(storageConfig);

            final var dataDistributor = this.getClusterStorageBinaryDataDistributor();
            DistributedStorage.configureWriting(embeddedStorageFoundation, dataDistributor);

            embeddedStorageFoundation.setExceptionHandler((throwable, channel) ->
            {
                try
                {
                    StorageExceptionHandler.defaultHandleException(throwable, channel);
                }
                catch (final StorageException exception)
                {
                    GlobalErrorHandling.handleFatalError(exception);
                }
            });

            final var embeddedStorageManager = embeddedStorageFoundation.start();

            if (embeddedStorageManager.root() == null)
            {
                throw new NodelibraryException("Storage-Node was started without a storage containing a root object");
            }

            final var storageLimitChecker = this.getStorageLimitChecker();
            storageLimitChecker.start();

            this.clusterStorageManager = ClusterStorageManager.New(embeddedStorageManager, storageLimitChecker);

            this.getClusterStorageBinaryDataClient().start();

            this.getStorageNodeHealthCheck().init();

            this.clusterRequestController = ClusterRestRequestController.StorageNode(
                this.getStorageNodeManager(),
                this.getNodelibraryPropertiesProvider()
            );
        }
    }
}
