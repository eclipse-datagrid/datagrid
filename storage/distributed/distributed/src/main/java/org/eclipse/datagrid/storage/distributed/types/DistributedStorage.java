package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse DataGrid Storage Distributed
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

import org.eclipse.store.storage.embedded.types.EmbeddedStorageConnectionFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;

import org.eclipse.datagrid.storage.distributed.internal.DistributedStorageConfigurator;

public final class DistributedStorage
{
	public static EmbeddedStorageFoundation<?> configureWriting(
		final EmbeddedStorageFoundation<?> foundation ,
		final StorageBinaryDataDistributor distributor
	)
	{
		final EmbeddedStorageConnectionFoundation<?> connectionFoundation = foundation.getConnectionFoundation();
		connectionFoundation.setInstanceDispatcher(new DistributedStorageConfigurator(distributor));
		return foundation;
	}
	
	
	private DistributedStorage()
	{
		throw new UnsupportedOperationException();
	}
}
