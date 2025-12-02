package org.eclipse.datagrid.storage.distributed.kafka.types;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Headers;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

public final class StorageBinaryDistributedKafka
{
	public static String keyMessageType()
	{
		return "message-type";
	}

	public static String keyMessageLength()
	{
		return "message-length";
	}

	public static String keyPacketCount()
	{
		return "packet-count";
	}

	public static String keyPacketIndex()
	{
		return "packet-index";
	}

	public static int maxPacketSize()
	{
		return 1_000_000;
	}

	public static Charset charset()
	{
		return StandardCharsets.UTF_8;
	}

	public static void addPacketHeaders(
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

	public static MessageType messageType(final Headers headers)
	{
		return MessageType.valueOf(deserializeString(headers.lastHeader(keyMessageType()).value()));
	}

	public static int messageLength(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyMessageLength()).value());
	}

	public static int packetIndex(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketIndex()).value());
	}

	public static int packetCount(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketCount()).value());
	}

	public static byte[] serialize(final String value)
	{
		return value.getBytes(charset());
	}

	public static String deserializeString(final byte[] bytes)
	{
		return new String(bytes, charset());
	}

	public static byte[] serialize(final int value)
	{
		return new byte[] {
			(byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value
		};
	}

	public static int deserializeInt(final byte[] bytes)
	{
		int value = 0;
		for (final byte b : bytes)
		{
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
