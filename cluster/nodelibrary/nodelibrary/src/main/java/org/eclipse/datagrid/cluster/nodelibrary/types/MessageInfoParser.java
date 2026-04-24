package org.eclipse.datagrid.cluster.nodelibrary.types;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.collections.EqHashTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface MessageInfoParser
{
    static MessageInfoParser New()
    {
        return new Default();
    }

    MessageInfo parseMessageInfo(final String offsetFileContent) throws NodelibraryException;

    class Default implements MessageInfoParser
    {
        private static final Logger LOG = LoggerFactory.getLogger(MessageInfoParser.class);

        @Override
        public MessageInfo parseMessageInfo(final String offsetFileContent) throws NodelibraryException
        {
            final String[] rows = offsetFileContent.trim().split("\n");
            try
            {
                // parse message index
                final long messageIndex = Long.parseLong(rows[0]);

                // parse partition offsets
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

                return MessageInfo.New(messageIndex, partitionOffsets.immure());
            }
            catch (final NumberFormatException | IndexOutOfBoundsException | NodelibraryException e)
            {
                if (e instanceof NodelibraryException)
                {
                    throw e;
                }
                throw new NodelibraryException("Failed to parse message info file", e);
            }
        }
    }
}
