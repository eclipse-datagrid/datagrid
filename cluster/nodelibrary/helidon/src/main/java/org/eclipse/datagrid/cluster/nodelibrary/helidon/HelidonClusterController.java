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
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRouteConfigurations;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;

import static org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRouteConfigurations.*;


@ApplicationScoped
@Path(ClusterRestRouteConfigurations.ROOT_PATH)
public class HelidonClusterController
{
    private final ClusterRestRequestController requestController;

    public HelidonClusterController(final ClusterRestRequestController requestController)
    {
        this.requestController = requestController;
    }

    @GET
    @Path(GetMicrostreamDistributor.PATH)
    @Produces(GetMicrostreamDistributor.PRODUCES)
    public boolean getMicrostreamDistributor() throws HttpResponseException
    {
        return this.requestController.getMicrostreamDistributor();
    }

    @POST
    @Path(PostMicrostreamActivateDistributorStart.PATH)
    @Consumes(PostMicrostreamActivateDistributorStart.CONSUMES)
    @Produces(PostMicrostreamActivateDistributorStart.PRODUCES)
    public void postMicrostreamActivateDistributorStart() throws HttpResponseException
    {
        this.requestController.postMicrostreamActivateDistributorStart();
    }

    @POST
    @Path(PostMicrostreamActivateDistributorFinish.PATH)
    @Consumes(PostMicrostreamActivateDistributorFinish.CONSUMES)
    @Produces(PostMicrostreamActivateDistributorFinish.PRODUCES)
    public boolean postMicrostreamActivateDistributorFinish() throws HttpResponseException
    {
        return this.requestController.postMicrostreamActivateDistributorFinish();
    }

    @GET
    @Path(GetMicrostreamHealth.PATH)
    @Produces(GetMicrostreamHealth.PRODUCES)
    public void getMicrostreamHealth() throws HttpResponseException
    {
        this.requestController.getMicrostreamHealth();
    }

    @GET
    @Path(GetMicrostreamHealthReady.PATH)
    @Produces(GetMicrostreamHealthReady.PRODUCES)
    public void getMicrostreamHealthReady() throws HttpResponseException
    {
        this.requestController.getMicrostreamHealthReady();
    }

    @GET
    @Path(GetMicrostreamStorageBytes.PATH)
    @Produces(GetMicrostreamStorageBytes.PRODUCES)
    public String getMicrostreamStorageBytes() throws HttpResponseException
    {
        return this.requestController.getMicrostreamStorageBytes();
    }

    @POST
    @Path(PostMicrostreamBackup.PATH)
    @Consumes(PostMicrostreamBackup.CONSUMES)
    @Produces(PostMicrostreamBackup.PRODUCES)
    public void postMicrostreamBackup(@RequestBody final PostMicrostreamBackup.Body body) throws HttpResponseException
    {
        this.requestController.postMicrostreamBackup(body);
    }

    @GET
    @Path(GetMicrostreamBackup.PATH)
    @Produces(GetMicrostreamBackup.PRODUCES)
    public boolean getMicrostreamBackup() throws HttpResponseException
    {
        return this.requestController.getMicrostreamBackup();
    }

    @POST
    @Path(PostMicrostreamUpdates.PATH)
    @Consumes(PostMicrostreamUpdates.CONSUMES)
    @Produces(PostMicrostreamUpdates.PRODUCES)
    public void postMicrostreamUpdates() throws HttpResponseException
    {
        this.requestController.postMicrostreamUpdates();
    }

    @GET
    @Path(GetMicrostreamUpdates.PATH)
    @Produces(GetMicrostreamUpdates.PRODUCES)
    public boolean getMicrostreamUpdates() throws HttpResponseException
    {
        return this.requestController.getMicrostreamUpdates();
    }

    @POST
    @Path(PostMicrostreamResumeUpdates.PATH)
    @Consumes(PostMicrostreamResumeUpdates.CONSUMES)
    @Produces(PostMicrostreamResumeUpdates.PRODUCES)
    public void postMicrostreamResumeUpdates() throws HttpResponseException
    {
        this.requestController.postMicrostreamResumeUpdates();
    }

    @POST
    @Path(PostMicrostreamGc.PATH)
    @Consumes(PostMicrostreamGc.CONSUMES)
    @Produces(PostMicrostreamGc.PRODUCES)
    public void postMicrostreamGc() throws HttpResponseException
    {
        this.requestController.postMicrostreamGc();
    }

    @GET
    @Path(GetMicrostreamGc.PATH)
    @Produces(GetMicrostreamGc.PRODUCES)
    public boolean getMicrostreamGc() throws HttpResponseException
    {
        return this.requestController.getMicrostreamGc();
    }
}
