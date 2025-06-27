package org.eclipse.datagrid.cluster.nodelibrary.common.storage;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary
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

import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.eclipse.serializer.chars.XChars.notEmpty;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.serializer.collections.BulkList;
import org.eclipse.serializer.collections.types.XList;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import org.eclipse.datagrid.cluster.nodelibrary.common.KafkaPropertiesProvider;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

public interface ClusterStorageBinaryDataDistributorKafka extends StorageBinaryDataDistributor
{
	static ClusterStorageBinaryDataDistributorKafka Sync(final String topicName)
	{
		return new ClusterStorageBinaryDataDistributorKafka.Sync(notEmpty(topicName));
	}

	static ClusterStorageBinaryDataDistributorKafka Async(final String topicName)
	{
		return new ClusterStorageBinaryDataDistributorKafka.Async(notEmpty(topicName));
	}

	void setStorageOffset(long offset);

	long getStorageOffset();

	abstract class Abstract implements ClusterStorageBinaryDataDistributorKafka
	{
		private final String topicName;
		private final KafkaProducer<String, byte[]> producer;
		private final AtomicLong                    storageOffset = new AtomicLong(Long.MIN_VALUE);

		Abstract(final String topicName)
		{
			this.topicName = topicName;

			final Properties properties = KafkaPropertiesProvider.provide();
			properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
			properties.setProperty(COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
			this.producer = new KafkaProducer<>(properties);
		}

		protected abstract void execute(Runnable action);

		private void distribute(final MessageType messageType, final Binary data)
		{
			this.execute(() -> this.executeDistribution(messageType, data));
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
			final int packetCount = messageSize / MyStorageBinaryDistributedKafka.maxPacketSize() + (messageSize
				% MyStorageBinaryDistributedKafka.maxPacketSize() == 0 ? 0 : 1);
			while (remaining > 0)
			{
				final byte[] packet = new byte[Math.min(remaining, MyStorageBinaryDistributedKafka.maxPacketSize())];
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

				final ProducerRecord<String, byte[]> record = new ProducerRecord<>(this.topicName, packet);
				MyStorageBinaryDistributedKafka.addPacketHeaders(
					record.headers(),
					messageType,
					messageSize,
					packetIndex,
					packetCount
				);

				final long offset = this.storageOffset.incrementAndGet();
				record.headers().add("storageOffset", Long.toString(offset).getBytes(StandardCharsets.UTF_8));

				producer.send(record);

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
					XMemory.toDirectByteBuffer(MyStorageBinaryDistributedKafka.serialize(typeDictionaryData))
				)
			);
		}

		@Override
		public synchronized void dispose()
		{
			if (this.producer != null)
			{
				this.producer.close();
			}
		}

		public void setStorageOffset(final long offset)
		{
			this.storageOffset.set(offset);
		}

		public long getStorageOffset()
		{
			return this.storageOffset.get();
		}
	}

	public static class Sync extends Abstract
	{
		Sync(final String topicName)
		{
			super(topicName);
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

		Async(final String topicName)
		{
			super(topicName);
			this.executor = Executors.newSingleThreadExecutor(this::createThread);
		}

		private Thread createThread(final Runnable runnable)
		{
			final Thread thread = new Thread(runnable);
			thread.setName("StorageDistributor-Kafka");
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
