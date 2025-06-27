package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse DataGrid Storage Distributed Kafka
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

import static org.eclipse.serializer.chars.XChars.notEmpty;
import static org.eclipse.serializer.util.X.notNull;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.serializer.collections.BulkList;
import org.eclipse.serializer.collections.types.XList;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;


public interface StorageBinaryDataDistributorKafka
       extends   StorageBinaryDataDistributor
{
	public static StorageBinaryDataDistributorKafka Sync(
		final Properties kafkaProperties,
		final String     topicName
	)
	{
		return new StorageBinaryDataDistributorKafka.Sync(
			notNull (kafkaProperties),
			notEmpty(topicName      )
		);
	}
	
	public static StorageBinaryDataDistributorKafka Async(
		final Properties kafkaProperties,
		final String     topicName
	)
	{
		return new StorageBinaryDataDistributorKafka.Async(
			notNull (kafkaProperties),
			notEmpty(topicName      )
		);
	}
	
	
	static abstract class Abstract implements StorageBinaryDataDistributorKafka
	{
		private final Properties                    kafkaProperties;
		private final String                        topicName      ;
		private       KafkaProducer<String, byte[]> kafkaProducer  ;

		Abstract(
			final Properties kafkaProperties,
			final String     topicName
		)
		{
			super();
			this.kafkaProperties = kafkaProperties;
			this.topicName       = topicName      ;
		}
		
		protected abstract void execute(Runnable action);
		
		private synchronized KafkaProducer<String, byte[]> ensureProducer()
		{
			return this.kafkaProducer != null
				?  this.kafkaProducer
				: (this.kafkaProducer = this.createProducer())
			;
		}
		
		private KafkaProducer<String, byte[]> createProducer()
		{
			final Properties properties = new Properties();
			properties.putAll(this.kafkaProperties);
			properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG  , StringSerializer   .class.getName());
			properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

			return new KafkaProducer<>(properties);
		}
		
		private void distribute(
			final MessageType messageType,
			final Binary      data
		)
		{
			this.execute(() -> this.executeDistribution(messageType, data));
		}
		
		private void executeDistribution(
			final MessageType messageType,
			final Binary      data
		)
		{
			final KafkaProducer<String, byte[]> producer    = this.ensureProducer();
			final ByteBuffer[]                  buffers     = this.allBuffers(data);
			int                                 messageSize = 0;
			for(final ByteBuffer buffer : buffers)
			{
				messageSize += buffer.remaining();
				buffer.mark();
			}
			
			int       remaining     = messageSize;
			int       currentBuffer = 0;
			int       packetIndex   = 0;
			final int packetCount   = messageSize / StorageBinaryDistributedKafka.maxPacketSize()
				+ (messageSize % StorageBinaryDistributedKafka.maxPacketSize() == 0 ? 0 : 1)
			;
			while(remaining > 0)
			{
				final byte[] packet = new byte[Math.min(
					remaining,
					StorageBinaryDistributedKafka.maxPacketSize()
				)];
				int packetOffset = 0;
				while(packetOffset < packet.length)
				{
					final ByteBuffer buffer = buffers[currentBuffer];
					final int        length = Math.min(
						packet.length - packetOffset,
						buffer.remaining()
					);
					buffer.get(packet, packetOffset, length);
					if(!buffer.hasRemaining())
					{
						currentBuffer++;
					}
					remaining    -= length;
					packetOffset += length;
				}
				
				final ProducerRecord<String, byte[]> record = new ProducerRecord<>(this.topicName, packet);
				StorageBinaryDistributedKafka.addPacketHeaders(
					record.headers(),
					messageType     ,
					messageSize     ,
					packetIndex     ,
					packetCount
				);
				producer.send(record);
				
				packetIndex++;
			}
			
			for(final ByteBuffer buffer : buffers)
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
			this.distribute(
				MessageType.DATA,
				data
			);
		}

		@Override
		public void distributeTypeDictionary(final String typeDictionaryData)
		{
			this.distribute(
				MessageType.TYPE_DICTIONARY,
				ChunksWrapper.New(
					XMemory.toDirectByteBuffer(
						StorageBinaryDistributedKafka.serialize(typeDictionaryData)
					)
				)
			);
		}
		
		@Override
		public synchronized void dispose()
		{
			if(this.kafkaProducer != null)
			{
				this.kafkaProducer.close();
			}
			
		}
		
	}
	
	
	public static class Sync extends Abstract
	{
		Sync(
			final Properties kafkaProperties,
			final String     topicName
		)
		{
			super(kafkaProperties, topicName);
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

		Async(
			final Properties kafkaProperties,
			final String     topicName
		)
		{
			super(kafkaProperties, topicName);
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
			if(!this.executor.isShutdown())
			{
				this.executor.shutdown();
			}
			
			super.dispose();
		}
		
	}
	
}
