package org.eclipse.datagrid.cluster.nodelibrary.types;

import java.nio.ByteBuffer;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

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

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;

import static org.eclipse.serializer.math.XMath.notNegative;
import static org.eclipse.serializer.math.XMath.positive;

import static org.eclipse.serializer.util.X.notNull;

public interface ClusterStorageBinaryDataPacket extends StorageBinaryDataPacket
{
	long messageIndex();

	public static ClusterStorageBinaryDataPacket New(
		final MessageType messageType,
		final int messageLength,
		final int packetIndex,
		final int packetCount,
		final long messageIndex,
		final ByteBuffer buffer
	)
	{
		return new Default(
			notNull(messageType),
			notNegative(messageLength),
			notNegative(packetIndex),
			positive(packetCount),
			messageIndex,
			notNull(buffer)
		);
	}

	public static class Default implements ClusterStorageBinaryDataPacket
	{
		private final MessageType messageType;
		private final int messageLength;
		private final int packetIndex;
		private final int packetCount;
		private final long messageIndex;
		private final ByteBuffer buffer;

		private Default(
			final MessageType messageType,
			final int messageLength,
			final int packetIndex,
			final int packetCount,
			final long messageIndex,
			final ByteBuffer buffer
		)
		{
			this.messageType = messageType;
			this.messageLength = messageLength;
			this.packetIndex = packetIndex;
			this.packetCount = packetCount;
			this.messageIndex = messageIndex;
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
		public long messageIndex()
		{
			return this.messageIndex;
		}

		@Override
		public ByteBuffer buffer()
		{
			return this.buffer;
		}
	}
}
