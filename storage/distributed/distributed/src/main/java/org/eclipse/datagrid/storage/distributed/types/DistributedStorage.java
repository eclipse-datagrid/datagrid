package org.eclipse.datagrid.storage.distributed.types;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageConnectionFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;

import org.eclipse.datagrid.storage.distributed.internal.DistributedStorageConfigurator;

public final class DistributedStorage
{
	public static EmbeddedStorageFoundation<?> configureWriting(
		final EmbeddedStorageFoundation<?> foundation,
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
