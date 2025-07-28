package org.eclipse.datagrid.cluster.nodelibrary.common;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.Supplier;

import org.eclipse.datagrid.cluster.nodelibrary.common.impl._default.DefaultStorageClusterControllerBase;
import org.eclipse.datagrid.cluster.nodelibrary.common.impl.micro.MicroStorageClusterControllerBase;

public abstract class StorageClusterControllerBase
{
	public static final String CONTROLLER_PATH = "/microstream-cluster-controller";

	private final Impl impl;

	protected StorageClusterControllerBase()
	{
		this(Optional.empty());
	}

	protected StorageClusterControllerBase(
		final Optional<Supplier<ClusterStorageManager<?>>> clusterStorageManagerSupplier
	)
	{
		if (ClusterEnv.isMicro())
		{
			this.impl = new MicroStorageClusterControllerBase(clusterStorageManagerSupplier);
		}
		else
		{
			this.impl = new DefaultStorageClusterControllerBase(clusterStorageManagerSupplier);
		}
	}

	protected boolean internalDistributionActive()
	{
		return this.impl.distributionActive();
	}
	
	protected void internalStartDistributorActivation()
	{
		this.impl.startDistributorActivation();
	}
	
	protected boolean internalFinishDistributorActivation()
	{
		return this.impl.finishDistributorActivation();
	}

	protected void internalUploadStorage(final InputStream storage) throws IOException
	{
		this.impl.uploadStorage(storage);
	}

	protected boolean isReady()
	{
		return this.impl.isReady();
	}

	protected void internalCreateBackupNow()
	{
		this.impl.createBackupNow();
	}
	
	protected void internalPostStopUpdates()
	{
		this.impl.postStopUpdates();
	}
	
	protected boolean internalGetStopUpdates()
	{
		return this.impl.getStopUpdates();
	}

	protected void internalCallGc()
	{
		this.impl.callGc();
	}
	
	protected boolean internalIsGcRunning()
	{
		return this.impl.isGcRunning();
	}

	protected String internalGetUsedUpStorageBytes()
	{
		return String.format(
			"# HELP cluster_storage_used_bytes How many bytes are currently used up by the storage.\n"
				+ "# TYPE cluster_storage_used_bytes gauge\n"
				+ "cluster_storage_used_bytes{namespace=\"%s\",pod=\"%s\"} %s",
			ClusterEnv.myNamespace(),
			ClusterEnv.myPodName(),
			StorageLimitChecker.get().currentStorageDirectorySizeBytes().toString()
		);
	}

	public interface Impl
	{
		boolean distributionActive();
		
		void startDistributorActivation();
		
		boolean finishDistributorActivation();

		void uploadStorage(InputStream storage) throws IOException;

		boolean isReady();

		void createBackupNow();
		
		void postStopUpdates();
		
		boolean getStopUpdates();

		void callGc();
		
		boolean isGcRunning();
	}
	
}
