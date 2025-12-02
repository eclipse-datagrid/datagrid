package org.eclipse.datagrid.cluster.nodelibrary.types;

import java.util.function.Supplier;

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

import org.apache.kafka.common.KafkaException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.store.storage.types.StorageController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface StorageNodeHealthCheck extends AutoCloseable
{
	boolean isReady() throws NodelibraryException;

	boolean isHealthy();

	@Override
	void close();

	void init() throws NodelibraryException;

	static StorageNodeHealthCheck New(
		final String topic,
		final String groupId,
		final StorageController storageController,
		final ClusterStorageBinaryDataClient dataClient,
		final KafkaPropertiesProvider kafkaPropertiesProvider
	)
	{
		return new Default(
			notNull(topic),
			notNull(groupId),
			notNull(storageController),
			notNull(dataClient),
			notNull(kafkaPropertiesProvider)
		);
	}

	final class Default implements StorageNodeHealthCheck
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageNodeHealthCheck.class);
		private static final long KAFKA_MESSAGE_LAG_TOLERANCE = 10;

		private final KafkaMessageInfoProvider kafka;
		private final StorageController storageController;
		private final ClusterStorageBinaryDataClient dataClient;

		private boolean isActive = true;

		private Default(
			final String topic,
			final String groupId,
			final StorageController storageController,
			final ClusterStorageBinaryDataClient dataClient,
			final KafkaPropertiesProvider kafkaPropertiesProvider
		)
		{
			this.kafka = KafkaMessageInfoProvider.New(topic, groupId, kafkaPropertiesProvider);
			this.storageController = storageController;
			this.dataClient = dataClient;
		}

		@Override
		public void init() throws NodelibraryException
		{
			this.tryRun(this.kafka::init);
		}

		@Override
		public boolean isHealthy()
		{
			return this.isStorageReady() && this.isActive;
		}

		@Override
		public boolean isReady() throws NodelibraryException
		{
			return this.isStorageReady() && this.isKafkaReady();
		}

		private void tryRun(final Runnable runnable) throws NodelibraryException
		{
			try
			{
				runnable.run();
			}
			catch (final KafkaException e)
			{
				this.close();
				throw new NodelibraryException(e);
			}
		}

		private <T> T tryRun(final Supplier<T> supplier) throws NodelibraryException
		{
			try
			{
				return supplier.get();
			}
			catch (final KafkaException e)
			{
				this.close();
				throw new NodelibraryException(e);
			}
		}

		private boolean isStorageReady()
		{
			return this.storageController.isRunning() && !this.storageController.isStartingUp();
		}

		private synchronized boolean isKafkaReady() throws KafkaException
		{
			if (!this.isActive)
			{
				return false;
			}

			return this.tryRun(() ->
			{
				final long latestMessageIndex = this.kafka.provideLatestMessageIndex();
				final long currentMessageIndex = this.dataClient.messageInfo().messageIndex();

				if (LOG.isTraceEnabled() && latestMessageIndex - currentMessageIndex > 1_000)
				{
					LOG.trace(
						"Current MessageIndex: {}, Latest MessageIndex: {}, Difference: {}",
						currentMessageIndex,
						latestMessageIndex,
						latestMessageIndex - currentMessageIndex
					);
				}

				if (currentMessageIndex >= latestMessageIndex)
				{
					return true;
				}

				return latestMessageIndex - currentMessageIndex <= KAFKA_MESSAGE_LAG_TOLERANCE;
			});
		}

		@Override
		public synchronized void close()
		{
			if (!this.isActive)
			{
				return;
			}

			LOG.info("Closing StorageNodeHealthCheck.");

			try
			{
				this.kafka.close();
			}
			catch (final KafkaException e)
			{
				LOG.error("Failed to close KafkaMessageInfoProvider", e);
			}

			this.isActive = false;
		}
	}
}
