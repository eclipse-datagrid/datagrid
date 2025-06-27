package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse DataGrid Storage Distributed Kafka
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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Headers;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

final class StorageBinaryDistributedKafka
{
	final static String keyMessageType()
	{
		return "message-type";
	}
	
	final static String keyMessageLength()
	{
		return "message-length";
	}
	
	final static String keyPacketCount()
	{
		return "packet-count";
	}
	
	final static String keyPacketIndex()
	{
		return "packet-index";
	}
	
	final static int maxPacketSize()
	{
		return 1_000_000;
	}
	
	final static Charset charset()
	{
		return StandardCharsets.UTF_8;
	}
	
	final static void addPacketHeaders(
		final Headers     headers    ,
		final MessageType messageType,
		final int         messageLength,
		final int         packetIndex,
		final int         packetCount
	)
	{
		headers.add(keyMessageType(),   serialize(messageType.name()));
		headers.add(keyMessageLength(), serialize(messageLength));
		headers.add(keyPacketIndex(),   serialize(packetIndex));
		headers.add(keyPacketCount(),   serialize(packetCount));
	}
	
	final static MessageType messageType(final Headers headers)
	{
		return MessageType.valueOf(
			deserializeString(headers.lastHeader(keyMessageType()).value())
		);
	}
	
	final static int messageLength(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyMessageLength()).value());
	}
	
	final static int packetIndex(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketIndex()).value());
	}
	
	final static int packetCount(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketCount()).value());
	}
	
	final static byte[] serialize(final String value)
	{
		return value.getBytes(charset());
	}
	
	final static String deserializeString(final byte[] bytes)
	{
		return new String(bytes, charset());
	}
	
	final static byte[] serialize(final int value)
	{
		return new byte[]
		{
            (byte)(value >>> 24),
            (byte)(value >>> 16),
            (byte)(value >>>  8),
            (byte) value
		};
	}
	
	final static int deserializeInt(final byte[] bytes)
	{
		int value = 0;
        for (final byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
	}
	
	
	private StorageBinaryDistributedKafka()
	{
		throw new UnsupportedOperationException();
	}
}
