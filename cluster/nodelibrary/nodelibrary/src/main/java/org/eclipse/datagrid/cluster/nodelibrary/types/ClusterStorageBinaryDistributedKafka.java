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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Headers;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

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

    public static String keyMicrostreamOffset()
    {
        return "microstreamOffset";
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
        final long microstreamOffset
    )
    {
        headers.add(keyMessageType(), serialize(messageType.name()));
        headers.add(keyMessageLength(), serializeInt(messageLength));
        headers.add(keyPacketIndex(), serializeInt(packetIndex));
        headers.add(keyPacketCount(), serializeInt(packetCount));
        headers.add(keyMicrostreamOffset(), serializeLong(microstreamOffset));
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

    public static long microstreamOffset(final Headers headers)
    {
        return deserializeLong(headers.lastHeader(keyMicrostreamOffset()).value());
    }

    public static byte[] serialize(final String value)
    {
        return value.getBytes(charset());
    }

    public static String deserializeString(final byte[] bytes)
    {
        return new String(bytes, charset());
    }

    public static byte[] serializeInt(final int value)
    {
        return new byte[] {
            (byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value
        };
    }

    public static byte[] serializeLong(final long value)
    {
        final var buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(value);
        return buf.array();
    }

    public static int deserializeInt(final byte[] bytes)
    {
        if (bytes.length != Integer.BYTES)
        {
            throw new RuntimeException("Expected byte array with size " + Integer.BYTES + ", received " + bytes.length);
        }

        int value = 0;
        for (final byte b : bytes)
        {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

    public static long deserializeLong(final byte[] bytes)
    {
        if (bytes.length != Long.BYTES)
        {
            throw new RuntimeException("Expected byte array with size " + Long.BYTES + ", received " + bytes.length);
        }

        return ByteBuffer.wrap(bytes).getLong();
    }

    private ClusterStorageBinaryDistributedKafka()
    {
        throw new UnsupportedOperationException();
    }
}
