package org.eclipse.datagrid.storage.distributed.types;

import static org.eclipse.serializer.math.XMath.notNegative;
import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

import java.nio.ByteBuffer;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

public interface StorageBinaryDataPacket
{
	public MessageType messageType();

	public int messageLength();

	public int packetIndex();

	public int packetCount();

	public ByteBuffer buffer();

	public static StorageBinaryDataPacket New(
		final MessageType messageType,
		final int messageLength,
		final int packetIndex,
		final int packetCount,
		final ByteBuffer buffer
	)
	{
		return new StorageBinaryDataPacket.Default(
			notNull(messageType),
			notNegative(messageLength),
			notNegative(packetIndex),
			positive(packetCount),
			notNull(buffer)
		);
	}

	public static class Default implements StorageBinaryDataPacket
	{
		private final MessageType messageType;
		private final int messageLength;
		private final int packetIndex;
		private final int packetCount;
		private final ByteBuffer buffer;

		Default(
			final MessageType messageType,
			final int messageLength,
			final int packetIndex,
			final int packetCount,
			final ByteBuffer buffer
		)
		{
			super();
			this.messageType = messageType;
			this.messageLength = messageLength;
			this.packetIndex = packetIndex;
			this.packetCount = packetCount;
			this.buffer = buffer;
		}

		@Override
		public MessageType messageType()
		{
			return this.messageType;
		}

		@Override
		public int messageLength()
		{
			return this.messageLength;
		}

		@Override
		public int packetIndex()
		{
			return this.packetIndex;
		}

		@Override
		public int packetCount()
		{
			return this.packetCount;
		}

		@Override
		public ByteBuffer buffer()
		{
			return this.buffer;
		}

	}

}
