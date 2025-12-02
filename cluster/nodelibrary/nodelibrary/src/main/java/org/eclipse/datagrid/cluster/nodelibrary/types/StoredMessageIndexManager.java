package org.eclipse.datagrid.cluster.nodelibrary.types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.afs.types.AWritableFile;
import org.eclipse.serializer.chars.VarString;
import org.eclipse.serializer.collections.EqHashTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface StoredMessageIndexManager extends AutoCloseable
{
	MessageInfo get() throws NodelibraryException;

	void set(MessageInfo messageInfoInfo) throws NodelibraryException;

	@Override
	void close();

	static StoredMessageIndexManager New(final AWritableFile messageInfoFile)
	{
		return new Default(notNull(messageInfoFile));
	}

	@FunctionalInterface
	interface Creator
	{
		StoredMessageIndexManager create(AWritableFile offsetFile);
	}

	final class Default implements StoredMessageIndexManager
	{
		private static final Logger LOG = LoggerFactory.getLogger(StoredMessageIndexManager.class);

		private final AWritableFile messageInfoFile;

		private boolean closed = false;
		private boolean initialized = false;

		private MessageInfo messageInfo;

		private Default(final AWritableFile messageInfoFile)
		{
			this.messageInfoFile = messageInfoFile;
		}

		@Override
		public MessageInfo get() throws NodelibraryException
		{
			this.ensureInit();
			return this.messageInfo;
		}

		@Override
		public void set(final MessageInfo messageInfo) throws NodelibraryException
		{
			this.ensureInit();

			final var str = VarString.New();
			str.add(messageInfo.messageIndex()).lf();
			messageInfo.kafkaPartitionOffsets()
				.forEach(
					entry -> str.add(entry.key().topic())
						.add(',')
						.add(entry.key().partition())
						.add(',')
						.add(entry.value().longValue())
						.lf()
				);

			final var buffer = ByteBuffer.wrap(str.encode());

			try
			{
				this.messageInfoFile.truncate(0);
				// for some reason 0x0a was added to the end once, maybe some afs weirdness?
				final long written = this.messageInfoFile.writeBytes(buffer);
				if (LOG.isDebugEnabled() && messageInfo.messageIndex() % 10_000 == 0)
				{
					LOG.debug("Stored message index {}, written {} bytes", messageInfo.messageIndex(), written);
				}
			}
			catch (final RuntimeException e)
			{
				throw new NodelibraryException("Failed to write message info file", e);
			}

			this.messageInfo = messageInfo;
		}

		private void ensureInit() throws NodelibraryException
		{
			if (this.initialized)
			{
				return;
			}

			LOG.info("Initializing StoredMessageIndexManager");

			final boolean createdNew = this.messageInfoFile.ensureExists();

			if (createdNew)
			{
				LOG.debug("New message info file has been created.");
				final EqHashTable<TopicPartition, Long> partitionOffsets = EqHashTable.New();
				this.messageInfo = MessageInfo.New(Long.MIN_VALUE, partitionOffsets.immure());
			}
			else
			{
				LOG.debug("Reading existing message info file.");
				final ByteBuffer fileBytesBuffer;
				try
				{
					fileBytesBuffer = this.messageInfoFile.readBytes();
				}
				catch (final NodelibraryException e)
				{
					throw new NodelibraryException("Failed to read message info file", e);
				}

				if (fileBytesBuffer.remaining() == 0)
				{
					LOG.debug("Previous message info file is empty");
					final EqHashTable<TopicPartition, Long> partitionOffsets = EqHashTable.New();
					this.messageInfo = MessageInfo.New(Long.MIN_VALUE, partitionOffsets.immure());
				}
				else
				{
					this.messageInfo = this.parseMessageInfo(fileBytesBuffer);
					LOG.debug("Read previous message index at {}", this.messageInfo.messageIndex());
				}
			}

			this.initialized = true;
		}

		private MessageInfo parseMessageInfo(final ByteBuffer buffer) throws NodelibraryException
		{
			final var bytes = new byte[buffer.remaining()];
			buffer.get(bytes);
			final String[] rows = new String(bytes, StandardCharsets.UTF_8).trim().split("\n");
			try
			{
				// parse message index
				final long messageIndex = Long.parseLong(rows[0]);

				// parse partition offsets
				final EqHashTable<TopicPartition, Long> partitionOffsets = EqHashTable.New();
				for (int i = 1; i < rows.length; i++)
				{
					final String[] cols = rows[i].split(",");
					if (cols.length != 3)
					{
						throw new NodelibraryException(
							"Offset Partition column formatting wrong, excpeted 3 comma separated columns: " + rows[i]
						);
					}

					final String topic = cols[0];
					final int partition = Integer.parseInt(cols[1]);
					final long offset = Long.parseLong(cols[2]);

					final var topicPartition = new TopicPartition(topic, partition);

					LOG.debug("Parsed partition {} at offset {}", topicPartition, offset);
					if (partitionOffsets.get(topicPartition) != null)
					{
						throw new NodelibraryException("Offset file contains duplicate partition " + partition);
					}
					partitionOffsets.put(topicPartition, offset);
				}

				return MessageInfo.New(messageIndex, partitionOffsets.immure());
			}
			catch (final NumberFormatException | IndexOutOfBoundsException | NodelibraryException e)
			{
				if (e instanceof NodelibraryException)
				{
					throw e;
				}
				throw new NodelibraryException("Failed to parse message info file", e);
			}
		}

		@Override
		public void close()
		{
			if (this.closed)
			{
				return;

			}
			LOG.trace("Closing StoredMessageIndexManager");

			try
			{
				this.messageInfoFile.release();
			}
			catch (final RuntimeException e)
			{
				LOG.error("Failed to release message info file", e);
			}

			this.closed = true;
		}
	}
}
