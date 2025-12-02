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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.serializer.collections.BulkList;
import org.eclipse.serializer.collections.types.XList;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.eclipse.serializer.chars.XChars.notEmpty;
import static org.eclipse.serializer.util.X.notNull;

public interface ClusterStorageBinaryDataDistributorKafka extends ClusterStorageBinaryDataDistributor
{
	static ClusterStorageBinaryDataDistributorKafka Sync(
		final String topicName,
		final KafkaPropertiesProvider kafkaPropertiesProvider
	)
	{
		return new ClusterStorageBinaryDataDistributorKafka.Sync(notEmpty(topicName), notNull(kafkaPropertiesProvider));
	}

	static ClusterStorageBinaryDataDistributorKafka Async(
		final String topicName,
		final KafkaPropertiesProvider kafkaPropertiesProvider
	)
	{
		return new ClusterStorageBinaryDataDistributorKafka.Async(
			notEmpty(topicName),
			notNull(kafkaPropertiesProvider)
		);
	}

	abstract class Abstract implements ClusterStorageBinaryDataDistributorKafka
	{
		private static final Logger LOG = LoggerFactory.getLogger(ClusterStorageBinaryDataDistributorKafka.class);

		private final String topicName;
		private final KafkaProducer<String, byte[]> producer;
		private long messageIndex = Long.MIN_VALUE;
		private boolean ignoreDistribution = false;

		protected Abstract(final String topicName, final KafkaPropertiesProvider kafkaPropertiesProvider)
		{
			this.topicName = topicName;

			final Properties properties = kafkaPropertiesProvider.provide();
			properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
			properties.setProperty(COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
			this.producer = new KafkaProducer<>(properties);
		}

		protected abstract void execute(Runnable action);

		private void distribute(final MessageType messageType, final Binary data)
		{
			if (this.ignoreDistribution)
			{
				LOG.trace("Ignoring distribution for data of type {}", messageType);
				return;
			}
			this.execute(() -> this.tryExecuteDistribution(messageType, data));
		}

		private void tryExecuteDistribution(final MessageType messageType, final Binary data)
		{
			try
			{
				this.executeDistribution(messageType, data);
			}
			catch (final Throwable t)
			{
				// handle error here instead of the call site since async distribution would not cause
				// the application to shut down
				GlobalErrorHandling.handleFatalError(t);
			}
		}

		private void executeDistribution(final MessageType messageType, final Binary data)
		{
			final ByteBuffer[] buffers = this.allBuffers(data);
			int messageSize = 0;

			for (final ByteBuffer buffer : buffers)
			{
				messageSize += buffer.remaining();
				buffer.mark();
			}

			int remaining = messageSize;
			int currentBuffer = 0;
			int packetIndex = 0;
			final int packetCount = messageSize / ClusterStorageBinaryDistributedKafka.maxPacketSize() + (messageSize
				% ClusterStorageBinaryDistributedKafka.maxPacketSize() == 0 ? 0 : 1);

			while (remaining > 0)
			{
				final byte[] packet = new byte[Math.min(
					remaining,
					ClusterStorageBinaryDistributedKafka.maxPacketSize()
				)];
				int packetOffset = 0;

				while (packetOffset < packet.length)
				{
					final ByteBuffer buffer = buffers[currentBuffer];
					final int length = Math.min(packet.length - packetOffset, buffer.remaining());

					buffer.get(packet, packetOffset, length);

					if (!buffer.hasRemaining())
					{
						currentBuffer++;
					}

					remaining -= length;
					packetOffset += length;
				}

				final var kafkaRecord = new ProducerRecord<String, byte[]>(this.topicName, packet);

				++this.messageIndex;

				ClusterStorageBinaryDistributedKafka.addPacketHeaders(
					kafkaRecord.headers(),
					messageType,
					messageSize,
					packetIndex,
					packetCount,
					this.messageIndex
				);

				if (LOG.isDebugEnabled() && this.messageIndex % 10_000 == 0)
				{
					LOG.debug("Sending kafka packet at message index {}", this.messageIndex);
				}
				this.producer.send(kafkaRecord);

				packetIndex++;
			}

			for (final ByteBuffer buffer : buffers)
			{
				buffer.reset();
			}
		}

		private ByteBuffer[] allBuffers(final Binary data)
		{
			final XList<ByteBuffer> list = BulkList.New();
			data.iterateChannelChunks(channelChunk -> list.addAll(channelChunk.buffers()));
			return list.toArray(ByteBuffer.class);
		}

		@Override
		public void distributeData(final Binary data)
		{
			this.distribute(MessageType.DATA, data);
		}

		@Override
		public void distributeTypeDictionary(final String typeDictionaryData)
		{
			this.distribute(
				MessageType.TYPE_DICTIONARY,
				ChunksWrapper.New(
					XMemory.toDirectByteBuffer(ClusterStorageBinaryDistributedKafka.serializeString(typeDictionaryData))
				)
			);
		}

		@Override
		public void messageIndex(final long index)
		{
			LOG.info("Setting distributor message index to {}", index);
			this.messageIndex = index;
		}

		@Override
		public long messageIndex()
		{
			return this.messageIndex;
		}

		@Override
		public boolean ignoreDistribution()
		{
			return this.ignoreDistribution;
		}

		@Override
		public void ignoreDistribution(final boolean ignore)
		{
			this.ignoreDistribution = ignore;
		}

		@Override
		public synchronized void dispose()
		{
			LOG.trace("Disposing data distributor");
			this.producer.close();
		}
	}

	public static class Sync extends Abstract
	{
		private Sync(final String topicName, final KafkaPropertiesProvider kafkaPropertiesProvider)
		{
			super(topicName, kafkaPropertiesProvider);
		}

		@Override
		protected void execute(final Runnable action)
		{
			action.run();
		}
	}

	public static class Async extends Abstract
	{
		private final ExecutorService executor;

		private Async(final String topicName, final KafkaPropertiesProvider kafkaPropertiesProvider)
		{
			super(topicName, kafkaPropertiesProvider);
			this.executor = Executors.newSingleThreadExecutor(this::createThread);
		}

		private Thread createThread(final Runnable runnable)
		{
			final Thread thread = new Thread(runnable);
			thread.setName("Eclipse-Datagrid-StorageDistributor-Kafka");
			return thread;
		}

		@Override
		protected void execute(final Runnable action)
		{
			this.executor.execute(action);
		}

		@Override
		public synchronized void dispose()
		{
			if (!this.executor.isShutdown())
			{
				this.executor.shutdown();
			}

			super.dispose();
		}
	}
}
