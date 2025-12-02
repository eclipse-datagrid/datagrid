package org.eclipse.datagrid.cluster.nodelibrary.types;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;

public interface ClusterNodeManager extends AutoCloseable
{
	@Override
	void close();

	void startStorageChecks();

	boolean isRunningStorageChecks();

	boolean isReady() throws NodelibraryException;

	boolean isHealthy();

	long readStorageSizeBytes() throws NodelibraryException;
}
