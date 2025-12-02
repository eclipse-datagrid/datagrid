package org.eclipse.datagrid.cluster.nodelibrary.types;

import org.eclipse.serializer.concurrency.LockedExecutor;
import org.eclipse.serializer.functional.Action;
import org.eclipse.serializer.functional.Producer;

public abstract class ClusterLockScope
{
	private final LockedExecutor executor;

	protected ClusterLockScope(final LockedExecutor executor)
	{
		this.executor = executor;
	}

	public <T> T read(final Producer<T> producer)
	{
		return this.executor.read(producer);
	}

	public void read(final Action action)
	{
		this.executor.read(action);
	}

	public <T> T write(final Producer<T> producer)
	{
		return this.executor.write(producer);
	}

	public void write(final Action action)
	{
		this.executor.write(action);
	}
}
