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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

public interface StorageBinaryDataPacketAcceptor extends Consumer<List<StorageBinaryDataPacket>>
{
	@Override
	public void accept(final List<StorageBinaryDataPacket> packet);
	
	
	public static StorageBinaryDataPacketAcceptor New(final StorageBinaryDataReceiver receiver)
	{
		return new StorageBinaryDataPacketAcceptor.Default(
			notNull(receiver)
		);
	}
	
	
	public static class Default implements StorageBinaryDataPacketAcceptor
	{
		private final StorageBinaryDataReceiver receiver;
		private       StorageBinaryDataMessage  message ;
		
		protected Default(final StorageBinaryDataReceiver receiver)
		{
			super();
			this.receiver = receiver;
		}
		
		@Override
		public synchronized void accept(final List<StorageBinaryDataPacket> packets)
		{
			final List<StorageBinaryDataMessage> completeMessages = new ArrayList<>();
			
			for(final StorageBinaryDataPacket packet : packets)
			{
				if(this.message == null)
				{
					this.message = StorageBinaryDataMessage.New(packet);
				}
				else
				{
					this.message.addPacket(packet);
				}
				
				if(this.message.isComplete())
				{
					completeMessages.add(this.message);
					this.message = null;
				}
			}
			
			if(!completeMessages.isEmpty())
			{
				this.handleCompleteMessages(completeMessages);
			}
		}

		private void handleCompleteMessages(final List<StorageBinaryDataMessage> messages)
		{
			// Join similiar messages and hand over to receiver
			try
			{
				StorageBinaryDataMessage last    = null;
				final List<ByteBuffer>   buffers = new ArrayList<>();
				for(final StorageBinaryDataMessage message : messages)
				{
					if(last != null && last.type() != message.type())
					{
						this.send(last, buffers);
						buffers.clear();
					}
					
					buffers.add(message.data());
					last = message;
				}
				
				this.send(last, buffers);
			}
			finally
			{
				messages.forEach(StorageBinaryDataMessage::dispose);
			}
		}

		@SuppressWarnings("incomplete-switch")
		private void send(final StorageBinaryDataMessage last, final List<ByteBuffer> buffers)
		{
			switch(last.type())
			{
				case DATA:
				{
					// join all buffers of previous data messages
					this.receiver.receiveData(ChunksWrapper.New(
						buffers.toArray(ByteBuffer[]::new)
					));
				}
				break;
				
				case TYPE_DICTIONARY:
				{
					// type dictionary is always sent completely, so only the last one is relevant
					this.receiver.receiveTypeDictionary(this.createTypeDictionary(last.data()));
				}
				break;
			}
		}

		private String createTypeDictionary(final ByteBuffer buffer)
		{
			return new String(
				XMemory.toArray(buffer),
				StandardCharsets.UTF_8
			);
		}
		
	}
	
}
