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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface AfterDataMessageConsumedListener extends AutoCloseable
{
    void onChange(OffsetInfo offsetInfo) throws NodelibraryException;

    @Override
    void close();

    static AfterDataMessageConsumedListener RunStorageChecks(final StorageConnection connection)
    {
        return new RunStorageChecks(notNull(connection));
    }

    static AfterDataMessageConsumedListener Combined(
        final AfterDataMessageConsumedListener first,
        final AfterDataMessageConsumedListener second
    )
    {
        return new Combined(notNull(first), notNull(second));
    }

    final class Combined implements AfterDataMessageConsumedListener
    {
        private final AfterDataMessageConsumedListener first;
        private final AfterDataMessageConsumedListener second;

        private Combined(final AfterDataMessageConsumedListener first, final AfterDataMessageConsumedListener second)
        {
            this.first = first;
            this.second = second;
        }

        @Override
        public void onChange(final OffsetInfo offsetInfo) throws NodelibraryException
        {
            this.first.onChange(offsetInfo);
            this.second.onChange(offsetInfo);
        }

        @Override
        public void close()
        {
            this.first.close();
            this.second.close();
        }
    }

    final class RunStorageChecks implements AfterDataMessageConsumedListener
    {
        private static final Logger LOG = LoggerFactory.getLogger(RunStorageChecks.class);

        private final StorageConnection connection;

        private RunStorageChecks(final StorageConnection connection)
        {
            this.connection = connection;
        }

        @Override
        public void onChange(final OffsetInfo offsetInfo) throws NodelibraryException
        {
            if (LOG.isTraceEnabled() && offsetInfo.msOffset() % 10_000 == 0)
            {
                LOG.trace("Issuing gc and fck at offset {}", offsetInfo.msOffset());
            }
            this.connection.issueGarbageCollection(1_000_000L);
            this.connection.issueFileCheck(1_000_000L);
        }

        @Override
        public void close()
        {
            // no-op
        }
    }
}
