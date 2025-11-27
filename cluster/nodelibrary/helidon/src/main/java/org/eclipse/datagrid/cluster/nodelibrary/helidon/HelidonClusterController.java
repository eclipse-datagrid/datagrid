package org.eclipse.datagrid.cluster.nodelibrary.helidon;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Helidon
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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.*;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;

import static org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.*;


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
    public boolean getDataGridDistributor() throws HttpResponseException
    {
        return this.requestController.getDataGridDistributor();
    }

    @POST
    @Path(PostActivateDistributorStart.PATH)
    @Consumes(PostActivateDistributorStart.CONSUMES)
    @Produces(PostActivateDistributorStart.PRODUCES)
    public void postDataGridActivateDistributorStart() throws HttpResponseException
    {
        this.requestController.postDataGridActivateDistributorStart();
    }

    @POST
    @Path(PostActivateDistributorFinish.PATH)
    @Consumes(PostActivateDistributorFinish.CONSUMES)
    @Produces(PostActivateDistributorFinish.PRODUCES)
    public boolean postDataGridActivateDistributorFinish() throws HttpResponseException
    {
        return this.requestController.postDataGridActivateDistributorFinish();
    }

    @GET
    @Path(GetHealth.PATH)
    @Produces(GetHealth.PRODUCES)
    public void getDataGridHealth() throws HttpResponseException
    {
        this.requestController.getDataGridHealth();
    }

    @GET
    @Path(GetHealthReady.PATH)
    @Produces(GetHealthReady.PRODUCES)
    public void getDataGridHealthReady() throws HttpResponseException
    {
        this.requestController.getDataGridHealthReady();
    }

    @GET
    @Path(GetStorageBytes.PATH)
    @Produces(GetStorageBytes.PRODUCES)
    public String getDataGridStorageBytes() throws HttpResponseException
    {
        return this.requestController.getDataGridStorageBytes();
    }

    @POST
    @Path(PostBackup.PATH)
    @Consumes(PostBackup.CONSUMES)
    @Produces(PostBackup.PRODUCES)
    public void postDataGridBackup(@RequestBody final PostBackup.Body body) throws HttpResponseException
    {
        this.requestController.postDataGridBackup(body);
    }

    @GET
    @Path(GetBackup.PATH)
    @Produces(GetBackup.PRODUCES)
    public boolean getDataGridBackup() throws HttpResponseException
    {
        return this.requestController.getDataGridBackup();
    }

    @POST
    @Path(PostUpdates.PATH)
    @Consumes(PostUpdates.CONSUMES)
    @Produces(PostUpdates.PRODUCES)
    public void postDataGridUpdates() throws HttpResponseException
    {
        this.requestController.postDataGridUpdates();
    }

    @GET
    @Path(GetUpdates.PATH)
    @Produces(GetUpdates.PRODUCES)
    public boolean getDataGridUpdates() throws HttpResponseException
    {
        return this.requestController.getDataGridUpdates();
    }

    @POST
    @Path(PostResumeUpdates.PATH)
    @Consumes(PostResumeUpdates.CONSUMES)
    @Produces(PostResumeUpdates.PRODUCES)
    public void postDataGridResumeUpdates() throws HttpResponseException
    {
        this.requestController.postDataGridResumeUpdates();
    }

    @POST
    @Path(PostGc.PATH)
    @Consumes(PostGc.CONSUMES)
    @Produces(PostGc.PRODUCES)
    public void postDataGridGc() throws HttpResponseException
    {
        this.requestController.postDataGridGc();
    }

    @GET
    @Path(GetGc.PATH)
    @Produces(GetGc.PRODUCES)
    public boolean getDataGridGc() throws HttpResponseException
    {
        return this.requestController.getDataGridGc();
    }
}
