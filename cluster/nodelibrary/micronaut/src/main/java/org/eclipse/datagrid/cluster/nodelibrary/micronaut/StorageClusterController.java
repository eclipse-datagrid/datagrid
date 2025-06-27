package org.eclipse.datagrid.cluster.nodelibrary.micronaut;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary Micronaut
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.StorageClusterControllerBase;

@Controller(StorageClusterControllerBase.CONTROLLER_PATH)
public class StorageClusterController extends StorageClusterControllerBase
{
	private final Logger logger = LoggerFactory.getLogger(StorageClusterController.class);

	public StorageClusterController(final ClusterStorageManager<?> storageManager)
	{
		super(Optional.of(() -> storageManager));
	}

	@Get("/microstream-distributor")
	public boolean distributionActive()
	{
		return this.internalDistributionActive();
	}

	@Post("/microstream-activate-distributor")
	public void activateDistributor()
	{
		this.internalActivateDistributor();
	}

	@Get("/microstream-health")
	public void checkHealth()
	{
	}
	
	@Get("/microstream-storage-bytes")
	@Produces(MediaType.TEXT_PLAIN)
	public String storageBytes()
	{
		return this.internalGetUsedUpStorageBytes();
	}

	@Get("/microstream-health/ready")
	public HttpResponse<Void> readyCheck()
	{
		return HttpResponse.status(this.isReady() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR);
	}

	@Post("/microstream-uploadStorage")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@ExecuteOn(TaskExecutors.IO)
	public void uploadStorage(@Body @NonNull final InputStream storage) throws IOException
	{
		this.internalUploadStorage(storage);
	}

	@Post("/microstream-backup")
	@Produces(MediaType.TEXT_PLAIN)
	public HttpResponse<?> createBackupNow()
	{
		try
		{
			this.internalCreateBackupNow();
			return HttpResponse.ok();
		}
		catch (final Exception e)
		{
			this.logger.error(e.getMessage(), e);
			return HttpResponse.serverError(e.getMessage());
		}
	}

	@Post("/microstream-updates")
	@Consumes(MediaType.TEXT_PLAIN)
	public void stopUpdates()
	{
		this.internalStopUpdates();
	}

	@Post("/microstream-gc")
	public void callGc()
	{
		this.internalCallGc();
	}
}
