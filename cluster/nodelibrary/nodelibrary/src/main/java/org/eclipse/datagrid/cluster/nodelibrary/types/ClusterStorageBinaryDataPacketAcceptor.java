package org.eclipse.datagrid.cluster.nodelibrary.types;

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


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAcceptor;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.typing.Disposable;

import static org.eclipse.serializer.util.X.notNull;

/**
 * A {@link StorageBinaryDataPacketAcceptor} that will not dispose the
 * {@link StorageBinaryDataMessage}s so they can be cached and disposed by the
 * {@link ClusterStorageBinaryDataMerger}. This class will also call
 * {@link #dispose()} on the {@link ClusterStorageBinaryDataMerger}
 */
public interface ClusterStorageBinaryDataPacketAcceptor extends StorageBinaryDataPacketAcceptor, Disposable
{
	public static ClusterStorageBinaryDataPacketAcceptor New(final ClusterStorageBinaryDataMerger merger)
	{
		return new Default(notNull(merger));
	}

	public static class Default implements ClusterStorageBinaryDataPacketAcceptor
	{
		private final ClusterStorageBinaryDataMerger merger;
		private StorageBinaryDataMessage message;

		protected Default(final ClusterStorageBinaryDataMerger merger)
		{
			super();
			this.merger = merger;
		}

		@Override
		public synchronized void accept(final List<StorageBinaryDataPacket> packets)
		{
			final List<StorageBinaryDataMessage> completeMessages = new ArrayList<>();

			for (final StorageBinaryDataPacket packet : packets)
			{
				if (this.message == null)
				{
					this.message = StorageBinaryDataMessage.New(packet);
				}
				else
				{
					this.message.addPacket(packet);
				}

				if (this.message.isComplete())
				{
					completeMessages.add(this.message);
					this.message = null;
				}
			}

			if (!completeMessages.isEmpty())
			{
				this.handleCompleteMessages(completeMessages);
			}
		}

		private void handleCompleteMessages(final List<StorageBinaryDataMessage> messages)
		{
			// Join similiar messages and hand over to receiver
			try
			{
				StorageBinaryDataMessage last = null;
				final List<ByteBuffer> buffers = new ArrayList<>();
				for (final StorageBinaryDataMessage message : messages)
				{
					if (last != null && last.type() != message.type())
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
				// don't deallocate here, instead in ClusterStorageBinaryDataMerger so we can cache them
				// without having to copy
				//messages.forEach(StorageBinaryDataMessage::dispose);
			}
		}

		@SuppressWarnings("incomplete-switch")
		private void send(final StorageBinaryDataMessage last, final List<ByteBuffer> buffers)
		{
			switch (last.type())
			{
			case DATA:
			{
				// join all buffers of previous data messages
				this.merger.receiveData(ChunksWrapper.New(buffers.toArray(ByteBuffer[]::new)));
			}
				break;

			case TYPE_DICTIONARY:
			{
				// type dictionary is always sent completely, so only the last one is relevant
				this.merger.receiveTypeDictionary(this.createTypeDictionary(last.data()));
			}
				break;
			}
		}

		private String createTypeDictionary(final ByteBuffer buffer)
		{
			return new String(XMemory.toArray(buffer), StandardCharsets.UTF_8);
		}

		@Override
		public void dispose()
		{
			this.merger.dispose();
		}
	}
}
