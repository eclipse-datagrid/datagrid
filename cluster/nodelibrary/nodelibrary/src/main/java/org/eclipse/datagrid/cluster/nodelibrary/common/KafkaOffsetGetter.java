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
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool to ask kafka for the last microstream offset available
 */
public class KafkaOffsetGetter
{
	private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetGetter.class);
	private static final long PARTITION_ASSIGNMENT_TIMEOUT_MS = 10_000L;
	private static final long PARTITION_POLL_TIMEOUT_MS = 10_000L;
	
	private KafkaConsumer<String, byte[]> createKafkaConsumer(final String groupId)
	{
		final Properties properties = KafkaPropertiesProvider.provide();
		properties.setProperty(GROUP_ID_CONFIG, groupId);
		properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.setProperty(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
		properties.setProperty(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
		return new KafkaConsumer<>(properties);
	}
	
	/**
	 * Creates a new kafka consumer and asks for the last message in the topic,
	 * returns the microstream offset of that message
	 */
	public long getLastStorageOffset(final String groupId, final String topic)
	{
		long lastMicrostreamOffset = Long.MIN_VALUE;
		
		try (final var consumer = this.createKafkaConsumer(groupId))
		{
			LOG.trace("Subscribing to topic {}", topic);
			consumer.subscribe(Collections.singleton(topic));
			
			LOG.trace("Polling consumer until we have partitions assigned.");
			final long startMs = System.currentTimeMillis();
			while (consumer.assignment().isEmpty())
			{
				if (System.currentTimeMillis() > startMs + PARTITION_ASSIGNMENT_TIMEOUT_MS)
				{
					throw new RuntimeException("Timed out waiting for topic partition assignment");
				}
				consumer.poll(Duration.ofMillis(PARTITION_POLL_TIMEOUT_MS));
			}
			
			for (final var partition : consumer.assignment())
			{
				LOG.trace(
					"Seeking to end offset for partition {} of topic {}",
					partition.partition(),
					partition.topic()
				);
				final long endOffset = consumer.position(partition);
				if (endOffset > 0)
				{
					consumer.seek(partition, endOffset - 1);
				}
			}
			
			LOG.trace("Polling latest messages for topic {}", topic);
			for (final var rec : consumer.poll(Duration.ofMillis(PARTITION_POLL_TIMEOUT_MS)))
			{
				final byte[] microstreamOffsetRaw = rec.headers().lastHeader("microstreamOffset").value();
				final long microstreamOffset = Long.parseLong(new String(microstreamOffsetRaw, StandardCharsets.UTF_8));
				if (microstreamOffset > lastMicrostreamOffset)
				{
					lastMicrostreamOffset = microstreamOffset;
				}
			}
		}
		
		return lastMicrostreamOffset;
	}
}
