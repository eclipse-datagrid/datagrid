package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.serializer.concurrency.LockedExecutor;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class ObjectGraphUpdateHandlerFactory
{
	@Singleton
	public ObjectGraphUpdateHandler objectGraphUpdateHandler(final LockedExecutor executor)
	{
		return updater -> executor.write(updater::updateObjectGraph);
	}
}
