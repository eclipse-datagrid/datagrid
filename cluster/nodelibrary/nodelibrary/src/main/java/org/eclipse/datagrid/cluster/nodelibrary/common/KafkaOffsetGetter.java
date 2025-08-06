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

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool to ask kafka for the last microstream offset available
 */
public class KafkaOffsetGetter implements AutoCloseable
{
	public static KafkaOffsetGetter forTopic(final String topic)
	{
		return new KafkaOffsetGetter(topic, topic + "-offsetgetter");
	}
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetGetter.class);
	private static final long PARTITION_ASSIGNMENT_TIMEOUT_MS = 10_000L;
	private static final Duration POLL_TIMEOUT = Duration.ofMillis(250L);
	
	private final KafkaConsumer<String, byte[]> kafka;
	private final String topic;
	
	public KafkaOffsetGetter(final String topic, final String groupInstanceId)
	{
		this.topic = topic;
		this.kafka = this.createKafkaConsumer(groupInstanceId);
	}
	
	private KafkaConsumer<String, byte[]> createKafkaConsumer(final String groupInstanceId)
	{
		final Properties properties = KafkaPropertiesProvider.provide();
		properties.setProperty(GROUP_ID_CONFIG, groupInstanceId);
		properties.setProperty(GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
		properties.setProperty(CLIENT_ID_CONFIG, groupInstanceId);
		properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.setProperty(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
		properties.setProperty(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
		return new KafkaConsumer<>(properties);
	}
	
	@Override
	public void close() throws InterruptException, KafkaException
	{
		this.kafka.close();
	}
	
	public void init() throws KafkaException
	{
		LOG.trace("Subscribing to topic {}", this.topic);
		this.kafka.subscribe(Collections.singleton(this.topic));
		
		LOG.trace("Polling consumer until we have partitions assigned.");
		final long startMs = System.currentTimeMillis();
		final long endMs = startMs + PARTITION_ASSIGNMENT_TIMEOUT_MS;
		while (this.kafka.assignment().isEmpty())
		{
			if (System.currentTimeMillis() > endMs)
			{
				throw new RuntimeException("Timed out waiting for topic partition assignment");
			}
			this.kafka.poll(POLL_TIMEOUT);
		}
	}
	
	/**
	 * Creates a new kafka consumer and asks for the last message in the topic,
	 * returns the microstream offset of that message
	 */
	public long getLastMicrostreamOffset() throws KafkaException
	{
		long lastMicrostreamOffset = Long.MIN_VALUE;
		
		this.seekToLastOffsets();
		
		LOG.trace("Polling latest messages for topic {}", this.topic);
		for (final var rec : this.kafka.poll(POLL_TIMEOUT))
		{
			final byte[] microstreamOffsetRaw = rec.headers().lastHeader("microstreamOffset").value();
			final long microstreamOffset = Long.parseLong(new String(microstreamOffsetRaw, StandardCharsets.UTF_8));
			if (microstreamOffset > lastMicrostreamOffset)
			{
				lastMicrostreamOffset = microstreamOffset;
			}
		}
		
		return lastMicrostreamOffset;
	}
	
	private void seekToLastOffsets() throws KafkaException
	{
		LOG.trace("Seeking to last partition messages");
		this.kafka.poll(POLL_TIMEOUT); // Polling to update assignments
		final var partitions = this.kafka.assignment();
		this.kafka.seekToEnd(partitions);
		for (final var partition : partitions)
		{
			LOG.debug("Seeking to end offset for partition {} of topic {}", partition.partition(), partition.topic());
			final long endOffset = this.kafka.position(partition);
			if (endOffset > 0)
			{
				this.kafka.seek(partition, endOffset - 1);
			}
		}
	}
}
