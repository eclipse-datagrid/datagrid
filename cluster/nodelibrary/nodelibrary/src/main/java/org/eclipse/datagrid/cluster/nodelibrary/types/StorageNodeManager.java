package org.eclipse.datagrid.cluster.nodelibrary.types;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NotADistributorException;
import org.eclipse.store.storage.types.StorageController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface StorageNodeManager extends ClusterNodeManager
{
	boolean isDistributor();

	void switchToDistribution();

	boolean finishDistributonSwitch() throws NotADistributorException;

	long getCurrentMessageIndex();

	long getLatestMessageIndex();

	static StorageNodeManager New(
		final ClusterStorageBinaryDataDistributor dataDistributor,
		final StorageTaskExecutor storageTaskExecutor,
		final ClusterStorageBinaryDataClient dataClient,
		final StorageNodeHealthCheck healthCheck,
		final StorageController storageController,
		final StorageDiskSpaceReader storageDiskSpaceReader,
		final KafkaMessageInfoProvider kafkaMessageInfoProvider
	)
	{
		return new Default(
			notNull(dataDistributor),
			notNull(storageTaskExecutor),
			notNull(dataClient),
			notNull(healthCheck),
			notNull(storageController),
			notNull(storageDiskSpaceReader),
			notNull(kafkaMessageInfoProvider)
		);
	}

	final class Default implements StorageNodeManager
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageNodeManager.class);

		private final ClusterStorageBinaryDataDistributor dataDistributor;
		private StorageTaskExecutor storageTaskExecutor;
		private final ClusterStorageBinaryDataClient dataClient;
		private final StorageNodeHealthCheck healthCheck;
		private final StorageController storageController;
		private final StorageDiskSpaceReader storageDiskSpaceReader;
		private final KafkaMessageInfoProvider kafkaMessageInfoProvider;

		private boolean isDistributor;
		private boolean isSwitchingToDistributor;

		public Default(
			final ClusterStorageBinaryDataDistributor dataDistributor,
			final StorageTaskExecutor storageTaskExecutor,
			final ClusterStorageBinaryDataClient dataClient,
			final StorageNodeHealthCheck healthCheck,
			final StorageController storageController,
			final StorageDiskSpaceReader storageDiskSpaceReader,
			final KafkaMessageInfoProvider kafkaMessageInfoProvider
		)
		{
			this.dataDistributor = dataDistributor;
			this.dataClient = dataClient;
			this.healthCheck = healthCheck;
			this.storageController = storageController;
			this.storageDiskSpaceReader = storageDiskSpaceReader;
			this.storageTaskExecutor = storageTaskExecutor;
			this.kafkaMessageInfoProvider = kafkaMessageInfoProvider;
		}

		@Override
		public void startStorageChecks()
		{
			this.storageTaskExecutor.runChecks();
		}

		@Override
		public boolean isRunningStorageChecks()
		{
			return this.storageTaskExecutor.isRunningChecks();
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
			this.dataClient.stopAtLatestMessage();
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

			final var messageInfo = this.dataClient.messageInfo();

			// once a node has been switched to distribution it will never become a reader node anymore
			this.healthCheck.close();
			this.dataClient.dispose();
			this.dataDistributor.messageIndex(messageInfo.messageIndex());

			this.isDistributor = true;
			this.isSwitchingToDistributor = false;
			return true;
		}

		@Override
		public long getCurrentMessageIndex()
		{
			if (this.isDistributor())
			{
				return this.dataDistributor.messageIndex();
			}
			else
			{
				return this.dataClient.messageInfo().messageIndex();
			}
		}

		@Override
		public long getLatestMessageIndex()
		{
			return this.kafkaMessageInfoProvider.provideLatestMessageIndex();
		}

		@Override
		public void close()
		{
			LOG.info("Closing StorageNodeManager");
			this.dataDistributor.dispose();
			this.dataClient.dispose();
			this.healthCheck.close();
		}
	}
}
