package org.eclipse.datagrid.cluster.nodelibrary.common.storage;

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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.serializer.afs.types.AWritableFile;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.KafkaOffsetGetter;
import org.eclipse.datagrid.cluster.nodelibrary.common.KafkaPropertiesProvider;
import org.eclipse.datagrid.cluster.nodelibrary.common.OffsetFileWriter;
import org.eclipse.datagrid.storage.distributed.kafka.types.StorageBinaryDataClientKafka;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAcceptor;

public class MyStorageBinaryDataClientKafka implements StorageBinaryDataClientKafka
{
	private static final Path STOP_FILE_PATH = Paths.get("/storage/stopped");
	private static final Path OFFSET_FILE_PATH = Paths.get("/storage/offset");

	private final Logger logger = LoggerFactory.getLogger(MyStorageBinaryDataClientKafka.class);
	private final AtomicBoolean active = new AtomicBoolean();
	private final AtomicBoolean finished = new AtomicBoolean();
	private final AtomicBoolean ready = new AtomicBoolean();

	private final AtomicLong                      storageOffset;
	private final StorageBinaryDataPacketAcceptor packetAcceptor;
	private final String topicName;
	private final String groupId;

	private boolean stopAtLatestOffset;
	private OffsetFileWriter offsetFileWriter;
	private AWritableFile offsetFile;

	public MyStorageBinaryDataClientKafka(
		final String topicName,
		final String groupId,
		final long storageOffset,
		final StorageBinaryDataPacketAcceptor packetAcceptor
	)
	{
		this.topicName = topicName;
		this.groupId        = groupId;
		this.storageOffset  = new AtomicLong(storageOffset);
		this.packetAcceptor = packetAcceptor;
	}

	public boolean isReady()
	{
		return this.ready.get();
	}

	public boolean isActive()
	{
		return this.active.get();
	}

	/**
	 * Useful to ensure that isReady() only returns true again if no more messages
	 * have been pulled
	 */
	public void unready()
	{
		this.ready.set(false);
	}

	public long getStorageOffset()
	{
		return this.storageOffset.get();
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
		this.logger.info("Starting reading with storage offset {}", this.storageOffset.get());

		if (ClusterEnv.isMicro() || ClusterEnv.isBackupNode())
		{
			this.offsetFile = NioFileSystem.New().ensureFile(OFFSET_FILE_PATH).useWriting();
			this.offsetFileWriter = new OffsetFileWriter(this.offsetFile);
		}

		final Properties properties = KafkaPropertiesProvider.provide();
		properties.setProperty(GROUP_ID_CONFIG, this.groupId);
		properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
		properties.setProperty(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

		try (final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties))
		{
			consumer.subscribe(Collections.singletonList(this.topicName));
			while (this.active.get())
			{
				if (!this.stopAtLatestOffset)
				{
					final var records = consumer.poll(Duration.ofSeconds(5));
					this.consume(records);
					// We are ready if there are no more initial records to be read
					if (records.isEmpty())
					{
						this.ready.set(true);
					}
				}
				else
				{
					this.active.set(false);

					this.logger.info("Checking latest storage offset...");
					final long stopAt;
					try (final var offsetGetter = KafkaOffsetGetter.forTopic(this.topicName))
					{
						offsetGetter.init();
						stopAt = offsetGetter.getLastMicrostreamOffset();
					}
					this.logger.info("Stopping at ms offset {}", stopAt);

					while (this.storageOffset.get() < stopAt)
					{
						this.consume(consumer.poll(Duration.ofSeconds(5)));
					}

					while (this.offsetFileWriter.lastWrittenOffset() < stopAt)
					{
						final var target = this.storageOffset.get();
						this.offsetFileWriter.tryWrite(target);
						try
						{
							Thread.sleep(1);
						}
						catch (final InterruptedException e)
						{
							throw new RuntimeException(e);
						}
					}
				}
				consumer.commitSync();
				
				this.logger.trace("Completed updates. Stopped at offset {}", this.storageOffset.get());
			}

			this.finished.set(true);
		}
	}

	private void consume(final ConsumerRecords<String, byte[]> records)
	{
		final List<StorageBinaryDataPacket> packets = new ArrayList<>();
		for(final ConsumerRecord<String, byte[]> record : records)
		{
			if(record.serializedValueSize() >= 0)
			{
				final Headers headers = record.headers();
				final long offset = Long.parseLong(
					new String(headers.lastHeader("storageOffset").value(), StandardCharsets.UTF_8)
				);
				if(offset > this.storageOffset.get())
				{
					this.storageOffset.set(offset);
					packets.add(this.createDataPacket(record, headers));
				}
			}
		}

		if (!packets.isEmpty())
		{
			this.packetAcceptor.accept(packets);
			if (this.offsetFile != null)
			{
				this.offsetFileWriter.tryWrite(this.storageOffset.get());
			}
		}
	}

	private StorageBinaryDataPacket createDataPacket(final ConsumerRecord<String, byte[]> record, final Headers headers)
	{
		return StorageBinaryDataPacket.New(
			MyStorageBinaryDistributedKafka.messageType(headers),
			MyStorageBinaryDistributedKafka.messageLength(headers),
			MyStorageBinaryDistributedKafka.packetIndex(headers),
			MyStorageBinaryDistributedKafka.packetCount(headers),
			ByteBuffer.wrap(record.value())
		);
	}

	/**
	 * Stop collecting updates after the last available offset in kafka has been
	 * reached. After the offset has been reached a file called 'stopped' will be
	 * created in the storage directory.
	 */
	public void stopAtLatestOffset()
	{
		this.stopAtLatestOffset = true;
	}

	public boolean isFinished()
	{
		return this.finished.get();
	}

	@Override
	public void dispose()
	{
		this.active.set(false);
		if (this.offsetFile != null)
		{
			this.offsetFile.close();
		}
	}
}
