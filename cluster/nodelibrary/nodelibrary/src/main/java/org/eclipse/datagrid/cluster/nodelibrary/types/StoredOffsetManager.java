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

import static org.eclipse.serializer.util.X.notNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.afs.types.AWritableFile;
import org.eclipse.serializer.chars.VarString;
import org.eclipse.serializer.collections.EqHashTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface StoredOffsetManager extends AutoCloseable
{
    OffsetInfo get() throws NodelibraryException;

    void set(OffsetInfo offsetInfo) throws NodelibraryException;

    @Override
    void close();

    static StoredOffsetManager New(final AWritableFile offsetFile)
    {
        return new Default(notNull(offsetFile));
    }

    @FunctionalInterface
    interface Creator
    {
        StoredOffsetManager create(AWritableFile offsetFile);
    }

    final class Default implements StoredOffsetManager
    {
        private static final Logger LOG = LoggerFactory.getLogger(StoredOffsetManager.class);

        private final AWritableFile offsetFile;

        private boolean closed = false;
        private boolean initialized = false;

        private OffsetInfo offsetInfo;

        private Default(final AWritableFile offsetFile)
        {
            this.offsetFile = offsetFile;
        }

        @Override
        public OffsetInfo get() throws NodelibraryException
        {
            this.ensureInit();
            return this.offsetInfo;
        }

        @Override
        public void set(final OffsetInfo offsetInfo) throws NodelibraryException
        {
            this.ensureInit();

            final var str = VarString.New();
            str.add(offsetInfo.msOffset()).lf();
            offsetInfo.kafkaPartitionOffsets()
                .forEach(
                    entry -> str.add(entry.key().topic())
                        .add(',')
                        .add(entry.key().partition())
                        .add(',')
                        .add(entry.value().longValue())
                        .lf()
                );

            final var buffer = ByteBuffer.wrap(str.encode());

            try
            {
                this.offsetFile.truncate(0);
                // for some reason 0x0a was added to the end once, maybe some afs weirdness?
                final long written = this.offsetFile.writeBytes(buffer);
                if (LOG.isDebugEnabled() && offsetInfo.msOffset() % 10_000 == 0)
                {
                    LOG.debug("Stored offset {}, written {} bytes", offsetInfo.msOffset(), written);
                }
            }
            catch (final RuntimeException e)
            {
                throw new NodelibraryException("Failed to write offset file", e);
            }

            this.offsetInfo = offsetInfo;
        }

        private void ensureInit() throws NodelibraryException
        {
            if (this.initialized)
            {
                return;
            }

            LOG.info("Initializing stored offset manager.");

            final boolean createdNew = this.offsetFile.ensureExists();

            if (createdNew)
            {
                LOG.debug("New offset file has been created.");
                final EqHashTable<TopicPartition, Long> partitionOffsets = EqHashTable.New();
                this.offsetInfo = OffsetInfo.New(Long.MIN_VALUE, partitionOffsets.immure());
            }
            else
            {
                LOG.debug("Reading existing offset file.");
                final ByteBuffer fileBytesBuffer;
                try
                {
                    fileBytesBuffer = this.offsetFile.readBytes();
                }
                catch (final NodelibraryException e)
                {
                    throw new NodelibraryException("Failed to read offset file", e);
                }

                if (fileBytesBuffer.remaining() == 0)
                {
                    LOG.debug("Previous offset file is empty");
                    final EqHashTable<TopicPartition, Long> partitionOffsets = EqHashTable.New();
                    this.offsetInfo = OffsetInfo.New(Long.MIN_VALUE, partitionOffsets.immure());
                }
                else
                {
                    this.offsetInfo = this.parseOffsetInfo(fileBytesBuffer);
                    LOG.debug("Read previous offset at {}", this.offsetInfo.msOffset());
                }
            }

            this.initialized = true;
        }

        private OffsetInfo parseOffsetInfo(final ByteBuffer buffer) throws NodelibraryException
        {
            final var bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            final String[] rows = new String(bytes, StandardCharsets.UTF_8).trim().split("\n");
            try
            {
                final long msOffset = Long.parseLong(rows[0]);
                final EqHashTable<TopicPartition, Long> partitionOffsets = EqHashTable.New();
                for (int i = 1; i < rows.length; i++)
                {
                    final String[] cols = rows[i].split(",");
                    if (cols.length != 3)
                    {
                        throw new NodelibraryException(
                            "Offset Partition column formatting wrong, excpeted 3 comma separated columns: " + rows[i]
                        );
                    }

                    final String topic = cols[0];
                    final int partition = Integer.parseInt(cols[1]);
                    final long offset = Long.parseLong(cols[2]);

                    final var topicPartition = new TopicPartition(topic, partition);

                    LOG.debug("Parsed partition {} at offset {}", topicPartition, offset);
                    if (partitionOffsets.get(topicPartition) != null)
                    {
                        throw new NodelibraryException("Offset file contains duplicate partition " + partition);
                    }
                    partitionOffsets.put(topicPartition, offset);
                }

                return OffsetInfo.New(msOffset, partitionOffsets.immure());
            }
            catch (final NumberFormatException | IndexOutOfBoundsException | NodelibraryException e)
            {
                if (e instanceof NodelibraryException)
                {
                    throw e;
                }
                throw new NodelibraryException("Failed to parse offset file", e);
            }
        }

        @Override
        public void close()
        {
            if (this.closed)
            {
                return;

            }
            LOG.trace("Closing stored offset manager");

            try
            {
                this.offsetFile.release();
            }
            catch (final RuntimeException e)
            {
                LOG.error("Failed to release  offset file", e);
            }

            this.closed = true;
        }
    }
}
