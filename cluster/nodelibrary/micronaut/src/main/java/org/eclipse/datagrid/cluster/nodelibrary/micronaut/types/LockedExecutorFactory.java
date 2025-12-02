package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

import org.eclipse.serializer.concurrency.LockedExecutor;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class LockedExecutorFactory
{
	@Singleton
	public LockedExecutor lockedExecutor()
	{
		return LockedExecutor.New();
	}
}
