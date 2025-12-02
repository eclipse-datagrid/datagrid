package org.eclipse.datagrid.storage.distributed.types;

import org.eclipse.serializer.concurrency.XThreads;

@FunctionalInterface
public interface ObjectGraphUpdateHandler
{
	public void objectGraphUpdateAvailable(ObjectGraphUpdater updater);

	public static ObjectGraphUpdateHandler Synchronized()
	{
		return updater -> XThreads.executeSynchronized(updater::updateObjectGraph);
	}

}
