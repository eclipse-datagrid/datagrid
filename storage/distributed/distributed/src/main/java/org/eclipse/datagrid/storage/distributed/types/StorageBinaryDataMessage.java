package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse DataGrid Storage Distributed
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

import static org.eclipse.serializer.util.X.notNull;

import java.nio.ByteBuffer;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.typing.Disposable;

public interface StorageBinaryDataMessage extends Disposable
{
	public static enum MessageType
	{
		TYPE_DICTIONARY,
		DATA
	}
	
	public MessageType type();
	
	public int length();
	
	public int packetCount();
	
	public StorageBinaryDataMessage addPacket(StorageBinaryDataPacket packet);
	
	public boolean isComplete();
	
	public ByteBuffer data();
	
	
	public static StorageBinaryDataMessage New(final StorageBinaryDataPacket initialPacket)
	{
		return new StorageBinaryDataMessage.Default(
			notNull(initialPacket)
		);
	}
	
	
	public static class Default implements StorageBinaryDataMessage
	{
		private final MessageType type           ;
		private final int         length         ;
		private final int         packetCount    ;
		private       int         receivedPackets = 0;
		private       ByteBuffer  buffer         ;
		
		Default(final StorageBinaryDataPacket initialPacket)
		{
			super();
			this.type        = initialPacket.messageType();
			this.length      = initialPacket.messageLength();
			this.packetCount = initialPacket.packetCount();
			this.buffer      = XMemory.allocateDirectNative(this.length);
			this.addPacket(initialPacket);
		}

		private void validateForAddition(final StorageBinaryDataPacket packet)
		{
			if(this.isComplete())
			{
				// TODO typed exception
				throw new RuntimeException("Data message already complete");
			}
			
			final MessageType expectedMessageType = this.type;
			if(packet.messageType() != expectedMessageType)
			{
				// TODO typed exception
				throw new RuntimeException("Invalid packet type, received " + packet.messageType() + ", expected " + expectedMessageType);
			}
			
			final int expectedPacketIndex = this.receivedPackets;
			if(packet.packetIndex() != expectedPacketIndex)
			{
				// TODO typed exception
				throw new RuntimeException("Invalid packet index, received " + packet.packetIndex() + ", expected " + expectedPacketIndex);
			}
		}
		
		private void internalAddPacket(final StorageBinaryDataPacket packet)
		{
			final ByteBuffer source = packet.buffer();
			source.mark();
			this.buffer.put(source);
			source.reset();
			
			this.receivedPackets++;
			
			if(this.isComplete())
			{
				this.buffer.flip();
			}
		}

		@Override
		public MessageType type()
		{
			return this.type;
		}

		@Override
		public int length()
		{
			return this.length;
		}

		@Override
		public int packetCount()
		{
			return this.packetCount;
		}

		@Override
		public synchronized StorageBinaryDataMessage addPacket(final StorageBinaryDataPacket packet)
		{
			this.validateForAddition(packet);
			this.internalAddPacket(packet);
			
			return this;
		}

		@Override
		public boolean isComplete()
		{
			return this.receivedPackets == this.packetCount;
		}

		@Override
		public ByteBuffer data()
		{
			if(!this.isComplete())
			{
				// TODO typed exception
				throw new RuntimeException("Data message not complete yet");
			}
			
			if(this.buffer == null)
			{
				// TODO typed exception
				throw new RuntimeException("Data message already disposed");
			}
			
			return this.buffer;
		}
		
		@Override
		public void dispose()
		{
			XMemory.deallocateDirectByteBuffer(this.buffer);
			this.buffer = null;
		}
		
	}
	
}
