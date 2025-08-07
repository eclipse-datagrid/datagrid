package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
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
	static String keyMessageType()
	{
		return "message-type";
	}
	
	static String keyMessageLength()
	{
		return "message-length";
	}
	
	static String keyPacketCount()
	{
		return "packet-count";
	}
	
	static String keyPacketIndex()
	{
		return "packet-index";
	}
	
	static int maxPacketSize()
	{
		return 1_000_000;
	}
	
	static Charset charset()
	{
		return StandardCharsets.UTF_8;
	}
	
	static void addPacketHeaders(
		final Headers headers,
		final MessageType messageType,
		final int messageLength,
		final int packetIndex,
		final int packetCount
	)
	{
		headers.add(keyMessageType(),   serialize(messageType.name()));
		headers.add(keyMessageLength(), serialize(messageLength));
		headers.add(keyPacketIndex(),   serialize(packetIndex));
		headers.add(keyPacketCount(),   serialize(packetCount));
	}
	
	static MessageType messageType(final Headers headers)
	{
		return MessageType.valueOf(
			deserializeString(headers.lastHeader(keyMessageType()).value())
		);
	}
	
	static int messageLength(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyMessageLength()).value());
	}
	
	static int packetIndex(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketIndex()).value());
	}
	
	static int packetCount(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketCount()).value());
	}
	
	static byte[] serialize(final String value)
	{
		return value.getBytes(charset());
	}
	
	static String deserializeString(final byte[] bytes)
	{
		return new String(bytes, charset());
	}
	
	static byte[] serialize(final int value)
	{
		return new byte[]
		{
            (byte)(value >>> 24),
            (byte)(value >>> 16),
            (byte)(value >>>  8),
            (byte) value
		};
	}
	
	static int deserializeInt(final byte[] bytes)
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
