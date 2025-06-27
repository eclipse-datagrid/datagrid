package org.eclipse.datagrid.cluster.nodelibrary.common;

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

import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool to ask kafka for the last storage offset available
 */
public class KafkaOffsetGetter
{
	private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetGetter.class);

	/**
	 * Creates a new kafka consumer and asks for the last message in the topic,
	 * returns the storage offset of that message
	 */
	public long getLastStorageOffset(final String groupId, final String topic)
	{
		long lastStorageOffset = Long.MIN_VALUE;

		final Properties properties = KafkaPropertiesProvider.provide();
		properties.setProperty(GROUP_ID_CONFIG, groupId);
		properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.setProperty(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
		properties.setProperty(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

		try (final var consumer = new KafkaConsumer<String, byte[]>(properties))
		{
			consumer.subscribe(Arrays.asList(topic));

			// Find the partition with the furthest offset
			final var furthestPartition = consumer.committed(consumer.assignment())
				.entrySet()
				.stream()
				.max((a, b) -> Long.compare(a.getValue().offset(), b.getValue().offset()))
				.orElse(null);

			// Nothing here yet
			if (furthestPartition == null)
			{
				LOG.warn("Kafka topic {} contains no partitions", topic);
				return lastStorageOffset;
			}
			else if (furthestPartition.getValue().offset() == 0)
			{
				LOG.warn("Kafka topic {} contains no commited messages", topic);
				return lastStorageOffset;
			}

			// Check out the last message (last commited offset - 1) for the furthest partition
			consumer.seek(furthestPartition.getKey(), furthestPartition.getValue().offset() - 1);

			final var records = consumer.poll(Duration.ofSeconds(5));
			if (records.isEmpty())
			{
				throw new RuntimeException("Failed to get last commited message from partition");
			}
			for (final var rec : records)
			{
				final long recordMsOffset = Long.parseLong(
					new String(rec.headers().lastHeader("storageOffset").value(), StandardCharsets.UTF_8)
				);
				lastStorageOffset = Math.max(lastStorageOffset, recordMsOffset);
			}
		}

		return lastStorageOffset;
	}
}
