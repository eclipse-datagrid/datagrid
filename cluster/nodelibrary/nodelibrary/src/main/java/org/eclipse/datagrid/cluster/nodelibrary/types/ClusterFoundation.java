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
import org.eclipse.datagrid.cluster.nodelibrary.types.cronjob.*;
import org.eclipse.datagrid.cluster.nodelibrary.types.cronjob.GcWorkaroundQuartzCronJobManager.GcWorkaroundQuartzCronJob;
import org.eclipse.datagrid.cluster.nodelibrary.types.cronjob.StorageBackupQuartzCronJobManager.StorageBackupQuartzCronJob;
import org.eclipse.datagrid.cluster.nodelibrary.types.cronjob.StorageLimitCheckerQuartzCronJobManager.StorageLimitCheckerQuartzCronJob;
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
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.function.Supplier;


public interface ClusterFoundation<F extends ClusterFoundation<?>> extends InstanceDispatcher
{
    StorageBackupBackend getStorageBackupBackend();

    F setStorageBackupBackend(StorageBackupBackend backend);

    StorageTaskExecutor getStorageTaskExecutor();

    F setStorageTaskExecutor(StorageTaskExecutor executor);

    StorageBackupTaskExecutor getStorageBackupTaskExecutor();

    F setStorageBackupTaskExecutor(StorageBackupTaskExecutor executor);

    BackupProxyHttpClient getBackupProxyHttpClient();

    F setBackupProxyHttpClient(BackupProxyHttpClient client);

    QuartzCronJobScheduler getQuartzCronJobScheduler();

    F setQuartzCronJobScheduler(QuartzCronJobScheduler scheduler);

    QuartzCronJobJobFactory getQuartzCronJobJobFactory();

    F setQuartzCronJobJobFactory(QuartzCronJobJobFactory factory);

    StorageBackupQuartzCronJobManager getStorageBackupQuartzCronJobManager();

    F setStorageBackupQuartzCronJobManager(StorageBackupQuartzCronJobManager manager);

    StorageLimitCheckerQuartzCronJobManager getStorageLimitCheckerQuartzCronJobManager();

    F setStorageLimitCheckerQuartzCronJobManager(StorageLimitCheckerQuartzCronJobManager manager);

    GcWorkaroundQuartzCronJobManager getGcWorkaroundQuartzCronJobManager();

    F setGcWorkaroundQuartzCronJobManager(GcWorkaroundQuartzCronJobManager manager);

    KafkaPropertiesProvider getKafkaPropertiesProvider();

    F setKafkaPropertiesProvider(KafkaPropertiesProvider provider);

    ClusterStorageBinaryDataPacketAcceptor getClusterStorageBinaryDataPacketAcceptor();

    F setClusterStorageBinaryDataPacketAcceptor(ClusterStorageBinaryDataPacketAcceptor acceptor);

    ClusterStorageBinaryDataMerger getClusterStorageBinaryDataMerger();

    F setClusterStorageBinaryDataMerger(ClusterStorageBinaryDataMerger merger);

    AfterDataMessageConsumedListener getAfterDataMessageConsumedListener();

    F setAfterDataMessageConsumedListener(AfterDataMessageConsumedListener listener);

    StoredOffsetManager getStoredOffsetManager();

    F setStoredOffsetManager(StoredOffsetManager manager);

    StorageBackupManager getStorageBackupManager();

    F setStorageBackupManager(StorageBackupManager manager);

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

        private StorageBackupBackend backupBackend;
        private BackupProxyHttpClient backupProxyHttpClient;
        private EmbeddedStorageFoundation<?> embeddedStorageFoundation;
        private QuartzCronJobScheduler cronJobScheduler;
        private QuartzCronJobJobFactory cronJobFactory;
        private StorageBackupQuartzCronJobManager backupCronjobManager;
        private GcWorkaroundQuartzCronJobManager gcWorkaroundManager;
        private StorageLimitCheckerQuartzCronJobManager limitCheckerManager;
        private BackupNodeManager backupNodeManager;
        private ClusterStorageBinaryDataClient dataClient;
        private ClusterStorageBinaryDataDistributor dataDistributor;
        private MicroNodeManager microNodeManager;
        private StorageNodeHealthCheck healthCheck;
        private NodelibraryPropertiesProvider propertiesProvider;
        private StorageTaskExecutor storageTaskExecutor;
        private StorageBackupTaskExecutor storageBackupTaskExecutor;
        private StorageDiskSpaceReader storageDiskSpaceReader;
        private StorageNodeManager storageNodeManager;
        private boolean enableAsyncDistribution;
        private Supplier<Object> rootSupplier;
        private ObjectGraphUpdateHandler graphUpdateHandler;
        private StorageBackupManager storageBackupManager;
        private AfterDataMessageConsumedListener afterDataMessageConsumedListener;
        private ClusterStorageBinaryDataMerger dataMerger;
        private ClusterStorageBinaryDataPacketAcceptor dataPacketAcceptor;
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

        protected StorageBackupBackend ensureBackupBackend()
        {
            // TODO: Hardcoded path
            final var props = this.getNodelibraryPropertiesProvider();
            final StoredOffsetManager.Creator offsetManagerCreator = StoredOffsetManager::New;

            if (props.backupTarget() == BackupTarget.SAAS)
            {
                final var scratchSpace = Paths.get("/storage/backup/");
                if (!Files.exists(scratchSpace))
                {
                    try
                    {
                        Files.createDirectories(scratchSpace);
                    }
                    catch (final IOException e)
                    {
                        throw new NodelibraryException("Failed to create scratch space", e);
                    }
                }
                return NetworkArchiveBackupBackend.New(
                    scratchSpace,
                    this.getBackupProxyHttpClient(),
                    offsetManagerCreator
                );
            }
            else
            {
                return FilesystemVolumeBackupBackend.New(Paths.get("/backups"), offsetManagerCreator);
            }
        }

        protected StorageTaskExecutor ensureStorageTaskExecutor()
        {
            if (this.getNodelibraryPropertiesProvider().isBackupNode())
            {
                return this.getStorageBackupTaskExecutor();
            }
            return StorageTaskExecutor.New(this.clusterStorageManager);
        }

        protected StorageBackupTaskExecutor ensureStorageBackupTaskExecutor()
        {
            return StorageBackupTaskExecutor.New(this.clusterStorageManager, this.getStorageBackupManager());
        }

        protected StorageBackupQuartzCronJobManager ensureStorageBackupQuartzCronJobManager()
        {
            return StorageBackupQuartzCronJobManager.New(this.getStorageBackupManager());
        }

        protected BackupProxyHttpClient ensureBackupProxyHttpClient()
        {
            return BackupProxyHttpClient.New(
                URI.create(this.getNodelibraryPropertiesProvider().backupProxyServiceUrl())
            );
        }

        protected QuartzCronJobJobFactory ensureCronJobFactory()
        {
            return QuartzCronJobJobFactory.New();
        }

        protected QuartzCronJobScheduler ensureCronJobScheduler()
        {
            final Scheduler scheduler;
            try
            {
                scheduler = StdSchedulerFactory.getDefaultScheduler();
            }
            catch (final SchedulerException e)
            {
                throw new NodelibraryException("Failed to get the default quartz cron job scheduler", e);
            }
            return QuartzCronJobScheduler.New(scheduler);
        }

        protected GcWorkaroundQuartzCronJobManager ensureGcWorkaroundManager()
        {
            return GcWorkaroundQuartzCronJobManager.New(this.clusterStorageManager);
        }

        protected StorageLimitCheckerQuartzCronJobManager ensureStorageLimitCheckerManager()
        {
            return StorageLimitCheckerQuartzCronJobManager.New(
                this.getNodelibraryPropertiesProvider().storageLimitGB(),
                this.getStorageDiskSpaceReader()
            );
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
            return storedOffsetUpdater;
        }

        protected StorageBackupManager ensureStorageBackupManager()
        {
            final var props = this.getNodelibraryPropertiesProvider();
            final int maxBackupCount = props.keptBackupsCount();

            final Supplier<OffsetInfo> offsetProvider = this.getClusterStorageBinaryDataClient()::offsetInfo;

            return StorageBackupManager.New(
                this.clusterStorageManager,

                maxBackupCount,
                this.getStorageBackupBackend(),
                offsetProvider,
                this.getClusterStorageBinaryDataClient()
            );
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

        protected BackupNodeManager ensureBackupNodeManager()
        {
            return BackupNodeManager.New(
                this.getStorageBackupTaskExecutor(),

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
                this.getClusterStorageBinaryDataPacketAcceptor(),
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
                this.getStorageTaskExecutor(),
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
                this.getStorageTaskExecutor(),
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

        protected ClusterStorageBinaryDataMerger ensureClusterStorageBinaryDataMerger()
        {
            final Long mergerTimeoutMs = this.getNodelibraryPropertiesProvider().dataMergerTimeoutMs();
            final long cachingTimeoutMs = mergerTimeoutMs == null ? ClusterStorageBinaryDataMerger.Defaults
                .cachingTimeoutMs() : mergerTimeoutMs;
            return ClusterStorageBinaryDataMerger.New(
                this.getEmbeddedStorageFoundation().getConnectionFoundation(),
                this.clusterStorageManager,
                this.getObjectGraphUpdateHandler(),
                cachingTimeoutMs
            );
        }

        protected ClusterStorageBinaryDataPacketAcceptor ensureDataPacketAcceptor()
        {
            return ClusterStorageBinaryDataPacketAcceptor.New(this.getClusterStorageBinaryDataMerger());
        }

        @Override
        public StorageBackupBackend getStorageBackupBackend()
        {
            if (this.backupBackend == null)
            {
                this.backupBackend = this.dispatch(this.ensureBackupBackend());
            }
            return this.backupBackend;
        }

        @Override
        public F setStorageBackupBackend(final StorageBackupBackend backend)
        {
            this.backupBackend = backend;
            return this.$();
        }

        @Override
        public StorageTaskExecutor getStorageTaskExecutor()
        {
            if (this.storageTaskExecutor == null)
            {
                this.storageTaskExecutor = this.dispatch(this.ensureStorageTaskExecutor());
            }
            return this.storageTaskExecutor;
        }

        @Override
        public F setStorageTaskExecutor(final StorageTaskExecutor executor)
        {
            this.storageTaskExecutor = executor;
            return this.$();
        }

        @Override
        public StorageBackupTaskExecutor getStorageBackupTaskExecutor()
        {
            if (this.storageBackupTaskExecutor == null)
            {
                this.storageBackupTaskExecutor = this.dispatch(this.ensureStorageBackupTaskExecutor());
            }
            return this.storageBackupTaskExecutor;
        }

        @Override
        public F setStorageBackupTaskExecutor(final StorageBackupTaskExecutor executor)
        {
            this.storageBackupTaskExecutor = executor;
            return this.$();
        }

        @Override
        public StorageBackupQuartzCronJobManager getStorageBackupQuartzCronJobManager()
        {
            if (this.backupCronjobManager == null)
            {
                this.backupCronjobManager = this.dispatch(this.ensureStorageBackupQuartzCronJobManager());
            }
            return this.backupCronjobManager;
        }

        @Override
        public F setStorageBackupQuartzCronJobManager(final StorageBackupQuartzCronJobManager manager)
        {
            this.backupCronjobManager = manager;
            return this.$();
        }

        @Override
        public BackupProxyHttpClient getBackupProxyHttpClient()
        {
            if (this.backupProxyHttpClient == null)
            {
                this.backupProxyHttpClient = this.dispatch(this.ensureBackupProxyHttpClient());
            }
            return this.backupProxyHttpClient;
        }

        @Override
        public F setBackupProxyHttpClient(final BackupProxyHttpClient client)
        {
            this.backupProxyHttpClient = client;
            return this.$();
        }

        @Override
        public QuartzCronJobJobFactory getQuartzCronJobJobFactory()
        {
            if (this.cronJobFactory == null)
            {
                this.cronJobFactory = this.dispatch(this.ensureCronJobFactory());
            }
            return this.cronJobFactory;
        }

        @Override
        public F setQuartzCronJobJobFactory(final QuartzCronJobJobFactory factory)
        {
            this.cronJobFactory = factory;
            return this.$();
        }

        @Override
        public QuartzCronJobScheduler getQuartzCronJobScheduler()
        {
            if (this.cronJobScheduler == null)
            {
                this.cronJobScheduler = this.dispatch(this.ensureCronJobScheduler());
            }
            return this.cronJobScheduler;
        }

        @Override
        public F setQuartzCronJobScheduler(final QuartzCronJobScheduler scheduler)
        {
            this.cronJobScheduler = scheduler;
            return this.$();
        }

        @Override
        public GcWorkaroundQuartzCronJobManager getGcWorkaroundQuartzCronJobManager()
        {
            if (this.gcWorkaroundManager == null)
            {
                this.gcWorkaroundManager = this.dispatch(this.ensureGcWorkaroundManager());
            }
            return this.gcWorkaroundManager;
        }

        @Override
        public F setGcWorkaroundQuartzCronJobManager(final GcWorkaroundQuartzCronJobManager manager)
        {
            this.gcWorkaroundManager = manager;
            return this.$();
        }

        @Override
        public StorageLimitCheckerQuartzCronJobManager getStorageLimitCheckerQuartzCronJobManager()
        {
            if (this.limitCheckerManager == null)
            {
                this.limitCheckerManager = this.dispatch(this.ensureStorageLimitCheckerManager());
            }
            return this.limitCheckerManager;
        }

        @Override
        public F setStorageLimitCheckerQuartzCronJobManager(final StorageLimitCheckerQuartzCronJobManager manager)
        {
            this.limitCheckerManager = manager;
            return this.$();
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
        public ClusterStorageBinaryDataMerger getClusterStorageBinaryDataMerger()
        {
            if (this.dataMerger == null)
            {
                this.dataMerger = this.dispatch(this.ensureClusterStorageBinaryDataMerger());
            }
            return this.dataMerger;
        }

        @Override
        public F setClusterStorageBinaryDataMerger(final ClusterStorageBinaryDataMerger merger)
        {
            this.dataMerger = merger;
            return this.$();
        }

        @Override
        public ClusterStorageBinaryDataPacketAcceptor getClusterStorageBinaryDataPacketAcceptor()
        {
            if (this.dataPacketAcceptor == null)
            {
                this.dataPacketAcceptor = this.dispatch(this.ensureDataPacketAcceptor());
            }
            return this.dataPacketAcceptor;
        }

        @Override
        public F setClusterStorageBinaryDataPacketAcceptor(final ClusterStorageBinaryDataPacketAcceptor acceptor)
        {
            this.dataPacketAcceptor = acceptor;
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


            // TODO: Hardcoded paths
            final var storageParentPath = Paths.get("/storage/");
            final var storageRootPath = storageParentPath.resolve("storage");

            // if we use a downloaded storage, always scroll to the latest offset so we don't read old messages
            boolean useLatestOffset = false;
            boolean requiresStorageUpload = false;

            // don't send messages generated by starting the storage and storing the empty root
            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(true);

            final var backend = this.getStorageBackupBackend();

            final boolean containsBackups = backend.containsBackups();

            /*
             * If there are backups already available, use those instead as a fresh cluster
             * has none, but a upgraded cluster has the previous storage backed up
             */

            // user uploaded a new storage
            if (backend.hasUserUploadedStorage())
            {
                LOG.info("Downloading user uploaded storage");

                useLatestOffset = true;
                // since the storage is now different than before the storage nodes
                // also need the exact same storage
                requiresStorageUpload = true;
                this.deleteDirectory(storageRootPath);
                backend.downloadUserUploadedStorage(storageParentPath);
                backend.deleteUserUploadedStorage();
            }
            else if (containsBackups && !Files.exists(storageRootPath))
            {
                LOG.info("Downloading latest storage backup");
                backend.downloadLatestBackup(storageParentPath);
            }
            else
            {
                LOG.info("Starting with local storage");
            }

            if (useLatestOffset)
            {
                final var offsets = this.getKafkaOffsetProvider().provideLatestOffsetInfo();
                LOG.debug("Set starting offset to offsets: {}", offsets);
                this.getStoredOffsetManager().set(offsets);
            }

            LOG.info("Creating nodelibrary cluster controller");

            final var embeddedStorageFoundation = this.getEmbeddedStorageFoundation();
            // replace the storage live file provider from the provided embedded storage foundation
            StorageConfiguration storageConfig = embeddedStorageFoundation.getConfiguration();
            storageConfig = StorageConfiguration.Builder()
                .setBackupSetup(storageConfig.backupSetup())
                .setChannelCountProvider(storageConfig.channelCountProvider())
                .setDataFileEvaluator(storageConfig.dataFileEvaluator())
                .setEntityCacheEvaluator(storageConfig.entityCacheEvaluator())
                .setHousekeepingController(storageConfig.housekeepingController())
                .setStorageFileProvider(
                    StorageLiveFileProvider.New(NioFileSystem.New().ensureDirectory(storageRootPath))
                )
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
                LOG.debug("Setting and storing new root from root supplier");
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

            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(false);

            final var scheduler = this.getQuartzCronJobScheduler();

            this.clusterStorageManager = ClusterStorageManager.Wrapper(embeddedStorageManager, scheduler::shutdown);

            this.getClusterStorageBinaryDataClient().start();

            this.clusterRequestController = ClusterRestRequestController.BackupNode(
                this.getBackupNodeManager(),
                this.getNodelibraryPropertiesProvider()
            );

            final var jobFactory = this.getQuartzCronJobJobFactory();
            scheduler.setFactory(jobFactory);

            final var gcWorkaround = this.getGcWorkaroundQuartzCronJobManager();
            jobFactory.setJobFactory(GcWorkaroundQuartzCronJob.class, gcWorkaround::create);
            scheduler.schedule(
                JobBuilder.newJob(GcWorkaroundQuartzCronJob.class).withIdentity("GcWorkaround").build(),
                // Once every 30 minutes
                TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 */30 * * * ? *")).build()
            );

            final var storageBackup = this.getStorageBackupQuartzCronJobManager();
            jobFactory.setJobFactory(StorageBackupQuartzCronJob.class, storageBackup::create);
            scheduler.schedule(
                JobBuilder.newJob(StorageBackupQuartzCronJob.class).withIdentity("StorageBackup").build(),
                // Once at the start of every 2 hours
                TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 0 */2 * * ? *")).build()
            );

            // storage nodes need an initial backup to start from
            if (requiresStorageUpload)
            {
                LOG.info("Uploading starter backup for storage nodes");
                this.getBackupNodeManager().createStorageBackup(false);
            }

            scheduler.start();
        }

        protected void startStorageNode() throws NodelibraryException
        {
            LOG.info("Starting storage cluster node");

            // TODO: Hardcoded paths
            final var storageParentPath = Paths.get("/storage/");
            final var storageRootPath = storageParentPath.resolve("storage");
            final var offsetPath = storageParentPath.resolve("offset");

            if (Files.exists(storageRootPath))
            {
                LOG.info("Cleaning storage directory");
                this.deleteDirectory(storageRootPath);
            }

            if (Files.exists(offsetPath))
            {
                try
                {
                    Files.delete(offsetPath);
                }
                catch (final IOException e)
                {
                    throw new NodelibraryException("Failed to delete offset file", e);
                }
            }

            // don't send messages generated by starting the storage and storing the empty root
            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(true);

            final var backend = this.getStorageBackupBackend();
            final boolean containsBackups = backend.containsBackups();

            /*
             * If there are backups already available, use those instead
             */

            if (containsBackups)
            {
                LOG.info("Downloading latest storage backup");
                backend.downloadLatestBackup(storageParentPath);
            }
            else
            {
                LOG.info("Starting with local storage");
            }

            // don't send messages generated by starting the storage and storing the empty root
            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(true);

            this.getKafkaOffsetProvider().init();

            final var embeddedStorageFoundation = this.getEmbeddedStorageFoundation();
            // replace the storage live file provider from the provided embedded storage foundation
            StorageConfiguration storageConfig = embeddedStorageFoundation.getConfiguration();
            storageConfig = StorageConfiguration.Builder()
                .setBackupSetup(storageConfig.backupSetup())
                .setChannelCountProvider(storageConfig.channelCountProvider())
                .setDataFileEvaluator(storageConfig.dataFileEvaluator())
                .setEntityCacheEvaluator(storageConfig.entityCacheEvaluator())
                .setHousekeepingController(storageConfig.housekeepingController())
                .setStorageFileProvider(
                    StorageLiveFileProvider.New(NioFileSystem.New().ensureDirectory(storageRootPath))
                )
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
                LOG.debug("Setting and storing new root from root supplier");
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

            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(false);

            final var scheduler = this.getQuartzCronJobScheduler();

            this.clusterStorageManager = ClusterStorageManager.New(
                embeddedStorageManager,
                () -> this.getStorageLimitCheckerQuartzCronJobManager().limitReached(),
                scheduler::shutdown
            );

            this.getClusterStorageBinaryDataClient().start();

            this.getStorageNodeHealthCheck().init();

            this.clusterRequestController = ClusterRestRequestController.StorageNode(
                this.getStorageNodeManager(),
                this.getNodelibraryPropertiesProvider()
            );

            final var jobFactory = this.getQuartzCronJobJobFactory();

            final var gcWorkaround = this.getGcWorkaroundQuartzCronJobManager();
            jobFactory.setJobFactory(GcWorkaroundQuartzCronJob.class, gcWorkaround::create);

            final var limitChecker = this.getStorageLimitCheckerQuartzCronJobManager();
            jobFactory.setJobFactory(StorageLimitCheckerQuartzCronJob.class, limitChecker::create);

            scheduler.setFactory(jobFactory);
            scheduler.schedule(
                JobBuilder.newJob(GcWorkaroundQuartzCronJob.class).withIdentity("GcWorkaround").build(),
                // Once at the start of every hour
                TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 0 * * * ? *")).build()
            );
            scheduler.schedule(
                JobBuilder.newJob(StorageLimitCheckerQuartzCronJob.class).withIdentity("StorageLimitChecker").build(),
                TriggerBuilder.newTrigger()
                    .withSchedule(
                        SimpleScheduleBuilder.repeatMinutelyForever(
                            this.getNodelibraryPropertiesProvider().storageLimitCheckerIntervalMinutes()
                        )
                    )

                    .build()
            );

            scheduler.start();
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

            final var scheduler = this.getQuartzCronJobScheduler();

            this.clusterStorageManager = ClusterStorageManager.New(
                embeddedStorageManager,
                () -> this.getStorageLimitCheckerQuartzCronJobManager().limitReached(),
                scheduler::shutdown
            );
            this.clusterRequestController = ClusterRestRequestController.MicroNode(
                this.getMicroNodeManager(),





                this.getNodelibraryPropertiesProvider()
            );

            final var jobFactory = this.getQuartzCronJobJobFactory();




            final var limitChecker = this.getStorageLimitCheckerQuartzCronJobManager();
            jobFactory.setJobFactory(StorageLimitCheckerQuartzCronJob.class, limitChecker::create);

            scheduler.setFactory(jobFactory);





            scheduler.schedule(
                JobBuilder.newJob(StorageLimitCheckerQuartzCronJob.class).withIdentity("StorageLimitChecker").build(),
                TriggerBuilder.newTrigger()
                    .withSchedule(
                        SimpleScheduleBuilder.repeatMinutelyForever(
                            this.getNodelibraryPropertiesProvider().storageLimitCheckerIntervalMinutes()
                        )
                    )
                    .startNow()
                    .build()
            );

            scheduler.start();
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

            this.clusterStorageManager = ClusterStorageManager.Wrapper(
                storage,
                ClusterStorageManager.ShutdownCallback.NoOp()
            );
            this.clusterRequestController = ClusterRestRequestController.DevNode();
        }

        private void deleteDirectory(final Path path)
        {
            if (!Files.exists(path))
            {
                return;
            }

            LOG.info("Deleting files at {}", path);

            try (final var directories = Files.walk(path))
            {
                directories.sorted(Comparator.reverseOrder()).forEach(f ->
                {
                    try
                    {
                        Files.delete(f);
                    }
                    catch (final IOException e)
                    {
                        throw new NodelibraryException("Failed to delete file at " + f.toString(), e);
                    }
                });
            }
            catch (final IOException e) // thrown by Files.walk(Path)
            {
                throw new NodelibraryException("Failed to walk files at " + path);
            }
        }
    }
}
