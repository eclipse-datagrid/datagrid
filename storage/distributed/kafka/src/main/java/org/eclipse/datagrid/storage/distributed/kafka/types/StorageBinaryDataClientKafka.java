package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAcceptor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataReceiver;

import static org.eclipse.serializer.util.X.notNull;

public interface StorageBinaryDataClientKafka extends StorageBinaryDataClient
{
	public static StorageBinaryDataClientKafka New(
		final Properties kafkaProperties,
		final String topicName,
		final String clientId,
		final StorageBinaryDataReceiver receiver
	)
	{
		return New(kafkaProperties, topicName, clientId, StorageBinaryDataPacketAcceptor.New(receiver));
	}

	public static StorageBinaryDataClientKafka New(
		final Properties kafkaProperties,
		final String topicName,
		final String clientId,
		final StorageBinaryDataPacketAcceptor packetAcceptor
	)
	{
		return new StorageBinaryDataClientKafka.Default(
			notNull(kafkaProperties),
			notEmpty(topicName),
			notEmpty(clientId),
			notNull(packetAcceptor)
		);
	}

	public static class Default implements StorageBinaryDataClientKafka
	{
		private final Properties kafkaProperties;
		private final String topicName;
		private final String clientId;
		private final StorageBinaryDataPacketAcceptor packetAcceptor;
		private final AtomicBoolean active = new AtomicBoolean();

		Default(
			final Properties kafkaProperties,
			final String topicName,
			final String clientId,
			final StorageBinaryDataPacketAcceptor packetAcceptor
		)
		{
			super();
			this.kafkaProperties = kafkaProperties;
			this.topicName = topicName;
			this.clientId = clientId;
			this.packetAcceptor = packetAcceptor;
		}

		@Override
		public void start()
		{
			this.active.set(true);

			final Thread thread = new Thread(this::run);
			thread.setDaemon(true);
			thread.start();
		}

		private void run()
		{
			final Properties properties = new Properties();
			properties.putAll(this.kafkaProperties);
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.clientId);
			properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
			//		properties.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Duration.ofSeconds(3).toMillis());

			try (final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties))
			{
				consumer.subscribe(Collections.singletonList(this.topicName));
				while (this.active.get())
				{
					this.consume(consumer.poll(Duration.ofMillis(Long.MAX_VALUE)));
					consumer.commitSync();
				}
			}
		}

		private void consume(final ConsumerRecords<String, byte[]> records)
		{
			final List<StorageBinaryDataPacket> packets = new ArrayList<>();
			for (final ConsumerRecord<String, byte[]> record : records)
			{
				packets.add(this.createDataPacket(record));
			}
			if (!packets.isEmpty())
			{
				this.packetAcceptor.accept(packets);
			}
		}

		private StorageBinaryDataPacket createDataPacket(final ConsumerRecord<String, byte[]> record)
		{
			final Headers headers = record.headers();
			return StorageBinaryDataPacket.New(
				StorageBinaryDistributedKafka.messageType(headers),
				StorageBinaryDistributedKafka.messageLength(headers),
				StorageBinaryDistributedKafka.packetIndex(headers),
				StorageBinaryDistributedKafka.packetCount(headers),
				ByteBuffer.wrap(record.value())
			);
		}

		@Override
		public void dispose()
		{
			this.active.set(false);
		}

	}

}
