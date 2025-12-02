package org.eclipse.datagrid.cluster.nodelibrary.types;

import org.apache.kafka.common.header.Headers;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ClusterStorageBinaryDistributedKafka
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

	public static String keyMessageIndex()
	{
		return "messageIndex";
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
		final int packetCount,
		final long messageIndex
	)
	{
		headers.add(keyMessageType(), serializeString(messageType.name()));
		headers.add(keyMessageLength(), serializeInt(messageLength));
		headers.add(keyPacketIndex(), serializeInt(packetIndex));
		headers.add(keyPacketCount(), serializeInt(packetCount));
		headers.add(keyMessageIndex(), serializeLong(messageIndex));
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

	public static long messageIndex(final Headers headers)
	{
		return deserializeLong(headers.lastHeader(keyMessageIndex()).value());
	}

	public static byte[] serializeString(final String value)
	{
		return value.getBytes(charset());
	}

	public static String deserializeString(final byte[] bytes)
	{
		return new String(bytes, charset());
	}

	public static byte[] serializeInt(final int value)
	{
		return Integer.toString(value).getBytes(charset());
	}

	public static int deserializeInt(final byte[] bytes)
	{
		return Integer.parseInt(deserializeString(bytes));
	}

	public static byte[] serializeLong(final long value)
	{
		return Long.toString(value).getBytes(charset());
	}

	public static long deserializeLong(final byte[] bytes)
	{
		return Long.parseLong(deserializeString(bytes));
	}

	private ClusterStorageBinaryDistributedKafka()
	{
		throw new UnsupportedOperationException();
	}
}
