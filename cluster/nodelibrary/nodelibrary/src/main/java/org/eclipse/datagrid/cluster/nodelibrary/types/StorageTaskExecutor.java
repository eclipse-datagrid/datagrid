package org.eclipse.datagrid.cluster.nodelibrary.types;

import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public interface StorageTaskExecutor
{
	void runChecks();

	boolean isRunningChecks();

	static StorageTaskExecutor New(final StorageConnection connection)
	{
		return new Default(notNull(connection));
	}

	class Abstract implements StorageTaskExecutor
	{
		private static final Logger LOG = LoggerFactory.getLogger(Abstract.class);
		private final StorageConnection connection;

		private Thread checksThread;

		protected Abstract(final StorageConnection connection)
		{
			this.connection = connection;
		}

		@Override
		public void runChecks()
		{
			if (this.checksThread == null || !this.checksThread.isAlive())
			{
				LOG.debug("Issuing new storage checks");
				this.checksThread = new Thread(() ->
				{
					this.connection.issueFullGarbageCollection();
					this.connection.issueFullCacheCheck();
					this.connection.issueFullFileCheck();
				}, "EclipseStore-StorageChecks");
				this.checksThread.start();
			}
		}

		@Override
		public boolean isRunningChecks()
		{
			if (this.checksThread != null && !this.checksThread.isAlive())
			{
				LOG.trace("Cleanup previous storage checks thread");
				this.checksThread = null;
			}
			return this.checksThread != null;
		}
	}

	final class Default extends Abstract implements StorageTaskExecutor
	{
		private Default(final StorageConnection connection)
		{
			super(connection);
		}
	}
}
