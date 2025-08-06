package org.eclipse.datagrid.cluster.nodelibrary.common;

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

import java.util.function.Supplier;

import org.apache.kafka.common.KafkaException;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl._default.NodeDefaultClusterStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageKafkaReadiness implements AutoCloseable
{
	public static StorageKafkaReadiness fromEnv(final ClusterStorageManager<?> storageManager)
	{
		final var topic = ClusterEnv.kafkaTopicName();
		final var pod = ClusterEnv.myPodName();
		final var groupId = String.format("%s-%s-readiness", topic, pod);
		return new StorageKafkaReadiness(topic, groupId, storageManager);
	}
	
	private static final Logger LOG = LoggerFactory.getLogger(StorageKafkaReadiness.class);
	private static final long KAFKA_MESSAGE_LAG_TOLERANCE = 10;
	
	private final KafkaOffsetGetter kafka;
	private final ClusterStorageManager<?> storageManager;
	
	private boolean isActive = true;
	
	public StorageKafkaReadiness(
		final String topic,
		final String groupId,
		final ClusterStorageManager<?> storageManager
	)
	{
		this.kafka = new KafkaOffsetGetter(topic, groupId);
		this.storageManager = storageManager;
	}
	
	public void init() throws KafkaException
	{
		this.checkForKafkaExceptions(this.kafka::init);
	}
	
	public boolean isActive()
	{
		return this.isActive;
	}
	
	public boolean isReady() throws KafkaException
	{
		return this.isStorageReady() && this.isKafkaReady();
	}
	
	private void checkForKafkaExceptions(final Runnable runnable) throws KafkaException
	{
		try
		{
			runnable.run();
		}
		catch (final KafkaException e)
		{
			this.isActive = false;
			throw e;
		}
	}
	
	private <T> T checkForKafkaExceptions(final Supplier<T> supplier) throws KafkaException
	{
		try
		{
			return supplier.get();
		}
		catch (final KafkaException e)
		{
			this.isActive = false;
			throw e;
		}
	}
	
	private boolean isStorageReady()
	{
		return this.storageManager.isReady() && this.storageManager.isRunning();
	}
	
	private boolean isKafkaReady() throws KafkaException
	{
		return this.checkForKafkaExceptions(() ->
		{
			if (
				ClusterEnv.isBackupNode() || this.storageManager instanceof NodeDefaultClusterStorageManager<?>
					&& this.storageManager.isDistributor()
			)
			{
				return true;
			}
			
			final long latestOffset = this.kafka.getLastMicrostreamOffset();
			final long currentOffset = this.storageManager.getCurrentOffset();
			
			if (currentOffset >= latestOffset)
			{
				return true;
			}
			
			LOG.trace(
				"Current Offset: {}, Latest Offset: {}, Difference: {}",
				currentOffset,
				latestOffset,
				latestOffset - currentOffset
			);
			
			return currentOffset - latestOffset <= KAFKA_MESSAGE_LAG_TOLERANCE;
		});
	}
	
	@Override
	public void close()
	{
		try
		{
			this.kafka.close();
		}
		catch (final KafkaException e)
		{
			LOG.error("Failed to close kafka offset getter", e);
		}
		finally
		{
			this.isActive = false;
		}
	}
}
