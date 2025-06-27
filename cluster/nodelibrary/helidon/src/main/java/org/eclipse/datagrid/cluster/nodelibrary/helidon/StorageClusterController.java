package org.eclipse.datagrid.cluster.nodelibrary.helidon;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary Helidon
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

import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.StorageClusterControllerBase;

@ApplicationScoped
@Path(StorageClusterControllerBase.CONTROLLER_PATH)
public class StorageClusterController extends StorageClusterControllerBase
{
	private final Logger logger = LoggerFactory.getLogger(StorageClusterController.class);
	
	@Inject
	public StorageClusterController(@SuppressWarnings("rawtypes") final ClusterStorageManager storageManager)
	{
		super(Optional.of(() -> storageManager));
	}

	@GET
	@Path("/microstream-distributor")
	@Produces(MediaType.TEXT_PLAIN)
	public Response distributionActive()
	{
		return Response.ok(this.internalDistributionActive(), MediaType.TEXT_PLAIN).build();
	}

	@POST
	@Path("/microstream-activate-distributor")
	public Response activateDistributor()
	{
		this.internalActivateDistributor();
		return Response.ok().build();
	}

	@GET
	@Path("/microstream-health")
	public Response checkHealth()
	{
		return Response.ok().build();
	}
	
	@GET
	@Path("/microstream-storage-bytes")
	public Response storageBytes()
	{
		return Response.ok(this.internalGetUsedUpStorageBytes()).build();
	}

	@GET
	@Path("/microstream-health/ready")
	public Response readyCheck()
	{
		return Response.status(this.isReady() ? 200 : 500).build();
	}

	@POST
	@Path("/microstream-uploadStorage")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@RequestBody(required = true, content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM))
	public void uploadStorage(final InputStream storage) throws IOException
	{
		this.internalUploadStorage(storage);
	}

	@POST
	@Path("/microstream-backup")
	public Response createBackupNow()
	{
		return this.createVoidResponse(this::internalCreateBackupNow);
	}

	@POST
	@Path("/microstream-updates")
	@Consumes(MediaType.TEXT_PLAIN)
	public Response stopUpdates()
	{
		return this.createVoidResponse(this::internalStopUpdates);
	}

	@POST
	@Path("/microstream-gc")
	public Response callGc()
	{
		return this.createVoidResponse(this::internalCallGc);
	}

	private Response createVoidResponse(final Runnable call)
	{
		return this.createResponse(() ->
		{
			call.run();
			return null;
		});
	}

	private Response createResponse(final Supplier<?> bodySupplier)
	{
		try
		{
			return Response.ok(bodySupplier.get()).build();
		}
		catch (final Exception e)
		{
			this.logger.error(e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		}
	}
}
