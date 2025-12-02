package org.eclipse.datagrid.cluster.nodelibrary.helidon;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.GetBackup;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.GetDistributor;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.GetGc;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.GetHealth;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.GetHealthReady;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.GetStorageBytes;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.GetUpdates;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.PostActivateDistributorFinish;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.PostActivateDistributorStart;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.PostBackup;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.PostGc;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.PostResumeUpdates;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.PostUpdates;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

@ApplicationScoped
@Path(StorageNodeRestRouteConfigurations.ROOT_PATH)
public class HelidonClusterController
{
	private final ClusterRestRequestController requestController;

	public HelidonClusterController(final ClusterRestRequestController requestController)
	{
		this.requestController = requestController;
	}

	@GET
	@Path(GetDistributor.PATH)
	@Produces(GetDistributor.PRODUCES)
	public boolean getDistributor() throws HttpResponseException
	{
		return this.requestController.getDistributor();
	}

	@POST
	@Path(PostActivateDistributorStart.PATH)
	@Consumes(PostActivateDistributorStart.CONSUMES)
	@Produces(PostActivateDistributorStart.PRODUCES)
	public void postActivateDistributorStart() throws HttpResponseException
	{
		this.requestController.postActivateDistributorStart();
	}

	@POST
	@Path(PostActivateDistributorFinish.PATH)
	@Consumes(PostActivateDistributorFinish.CONSUMES)
	@Produces(PostActivateDistributorFinish.PRODUCES)
	public boolean postActivateDistributorFinish() throws HttpResponseException
	{
		return this.requestController.postActivateDistributorFinish();
	}

	@GET
	@Path(GetHealth.PATH)
	@Produces(GetHealth.PRODUCES)
	public void getHealth() throws HttpResponseException
	{
		this.requestController.getHealth();
	}

	@GET
	@Path(GetHealthReady.PATH)
	@Produces(GetHealthReady.PRODUCES)
	public void getHealthReady() throws HttpResponseException
	{
		this.requestController.getHealthReady();
	}

	@GET
	@Path(GetStorageBytes.PATH)
	@Produces(GetStorageBytes.PRODUCES)
	public String getStorageBytes() throws HttpResponseException
	{
		return this.requestController.getStorageBytes();
	}

	@POST
	@Path(PostBackup.PATH)
	@Consumes(PostBackup.CONSUMES)
	@Produces(PostBackup.PRODUCES)
	public void postBackup(@RequestBody final PostBackup.Body body) throws HttpResponseException
	{
		this.requestController.postBackup(body);
	}

	@GET
	@Path(GetBackup.PATH)
	@Produces(GetBackup.PRODUCES)
	public boolean getBackup() throws HttpResponseException
	{
		return this.requestController.getBackup();
	}

	@POST
	@Path(PostUpdates.PATH)
	@Consumes(PostUpdates.CONSUMES)
	@Produces(PostUpdates.PRODUCES)
	public void postUpdates() throws HttpResponseException
	{
		this.requestController.postUpdates();
	}

	@GET
	@Path(GetUpdates.PATH)
	@Produces(GetUpdates.PRODUCES)
	public boolean getUpdates() throws HttpResponseException
	{
		return this.requestController.getUpdates();
	}

	@POST
	@Path(PostResumeUpdates.PATH)
	@Consumes(PostResumeUpdates.CONSUMES)
	@Produces(PostResumeUpdates.PRODUCES)
	public void postResumeUpdates() throws HttpResponseException
	{
		this.requestController.postResumeUpdates();
	}

	@POST
	@Path(PostGc.PATH)
	@Consumes(PostGc.CONSUMES)
	@Produces(PostGc.PRODUCES)
	public void postGc() throws HttpResponseException
	{
		this.requestController.postGc();
	}

	@GET
	@Path(GetGc.PATH)
	@Produces(GetGc.PRODUCES)
	public boolean getGc() throws HttpResponseException
	{
		return this.requestController.getGc();
	}
}
