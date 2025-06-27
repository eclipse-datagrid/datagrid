package org.eclipse.datagrid.cluster.nodelibrary.common.storage;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary
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

public class MyStorageBinaryDistributedKafka
{
	public final static String keyMessageType()
	{
		return "message-type";
	}

	public final static String keyMessageLength()
	{
		return "message-length";
	}

	public final static String keyPacketCount()
	{
		return "packet-count";
	}

	public final static String keyPacketIndex()
	{
		return "packet-index";
	}

	public final static int maxPacketSize()
	{
		return 1_000_000;
	}

	public final static Charset charset()
	{
		return StandardCharsets.UTF_8;
	}

	public final static void addPacketHeaders(
		final Headers headers,
		final MessageType messageType,
		final int messageLength,
		final int packetIndex,
		final int packetCount
	)
	{
		headers.add(keyMessageType(), serialize(messageType.name()));
		headers.add(keyMessageLength(), serialize(messageLength));
		headers.add(keyPacketIndex(), serialize(packetIndex));
		headers.add(keyPacketCount(), serialize(packetCount));
	}

	public final static MessageType messageType(final Headers headers)
	{
		return MessageType.valueOf(deserializeString(headers.lastHeader(keyMessageType()).value()));
	}

	public final static int messageLength(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyMessageLength()).value());
	}

	public final static int packetIndex(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketIndex()).value());
	}

	public final static int packetCount(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketCount()).value());
	}

	public final static byte[] serialize(final String value)
	{
		return value.getBytes(charset());
	}

	public final static String deserializeString(final byte[] bytes)
	{
		return new String(bytes, charset());
	}

	public final static byte[] serialize(final int value)
	{
		return new byte[] {
			(byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value
		};
	}

	public final static int deserializeInt(final byte[] bytes)
	{
		int value = 0;
		for (final byte b : bytes)
		{
			value <<= 8;
			value |= b & 0xFF;
		}
		return value;
	}

	private MyStorageBinaryDistributedKafka()
	{
		throw new UnsupportedOperationException();
	}
}
