package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Micronaut
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

import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRouteConfigurations;

import static org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRouteConfigurations.*;


@Controller(ClusterRestRouteConfigurations.ROOT_PATH)
public class MicronautClusterController
{
    private final ClusterRestRequestController controller;

    public MicronautClusterController(final ClusterRestRequestController controller)
    {
        this.controller = controller;
    }

    @io.micronaut.http.annotation.Error
    public HttpResponse<Void> handleNodelibraryException(final HttpResponseException e)
    {
        final MutableHttpResponse<Void> response = HttpResponse.status(HttpStatus.valueOf(e.statusCode()));
        for (final var header : e.extraHeaders())
        {
            response.header(header.key(), header.value());
        }
        return response;
    }

    @Get(value = GetMicrostreamDistributor.PATH, produces = GetMicrostreamDistributor.PRODUCES)
    public boolean getMicrostreamDistributor() throws HttpResponseException
    {
        return this.controller.getMicrostreamDistributor();
    }

    @Post(
        value = PostMicrostreamActivateDistributorStart.PATH,
        consumes = PostMicrostreamActivateDistributorStart.CONSUMES,
        produces = PostMicrostreamActivateDistributorStart.PRODUCES
    )
    public void postMicrostreamActivateDistributorStart() throws HttpResponseException
    {
        this.controller.postMicrostreamActivateDistributorStart();
    }

    @Post(
        value = PostMicrostreamActivateDistributorFinish.PATH,
        consumes = PostMicrostreamActivateDistributorFinish.CONSUMES,
        produces = PostMicrostreamActivateDistributorFinish.PRODUCES
    )
    public boolean postMicrostreamActivateDistributorFinish() throws HttpResponseException
    {
        return this.controller.postMicrostreamActivateDistributorFinish();
    }

    @Get(value = GetMicrostreamHealth.PATH, produces = GetMicrostreamHealth.PRODUCES)
    public void getMicrostreamHealth() throws HttpResponseException
    {
        this.controller.getMicrostreamHealth();
    }

    @Get(value = GetMicrostreamHealthReady.PATH, produces = GetMicrostreamHealthReady.PRODUCES)
    @ExecuteOn(TaskExecutors.IO)
    public void getMicrostreamHealthReady() throws HttpResponseException
    {
        this.controller.getMicrostreamHealthReady();
    }

    @Get(value = GetMicrostreamStorageBytes.PATH, produces = GetMicrostreamStorageBytes.PRODUCES)
    @ExecuteOn(TaskExecutors.IO)
    public String getMicrostreamStorageBytes() throws HttpResponseException
    {
        return this.controller.getMicrostreamStorageBytes();
    }

    @Post(
        value = PostMicrostreamBackup.PATH,
        consumes = PostMicrostreamBackup.CONSUMES,
        produces = PostMicrostreamBackup.PRODUCES
    )
    public void postMicrostreamBackup(@Body final PostMicrostreamBackup.Body body) throws HttpResponseException
    {
        this.controller.postMicrostreamBackup(body);
    }

    @Get(value = GetMicrostreamBackup.PATH, produces = GetMicrostreamBackup.PRODUCES)
    public boolean getMicrostreamBackup() throws HttpResponseException
    {
        return this.controller.getMicrostreamBackup();
    }

    @Post(
        value = PostMicrostreamUpdates.PATH,
        consumes = PostMicrostreamUpdates.CONSUMES,
        produces = PostMicrostreamUpdates.PRODUCES
    )
    public void postMicrostreamUpdates() throws HttpResponseException
    {
        this.controller.postMicrostreamUpdates();
    }

    @Get(value = GetMicrostreamUpdates.PATH, produces = GetMicrostreamUpdates.PRODUCES)
    public boolean getMicrostreamUpdates() throws HttpResponseException
    {
        return this.controller.getMicrostreamUpdates();
    }

    @Post(
        value = PostMicrostreamResumeUpdates.PATH,
        consumes = PostMicrostreamResumeUpdates.CONSUMES,
        produces = PostMicrostreamResumeUpdates.PRODUCES
    )
    public void postMicrostreamResumeUpdates() throws HttpResponseException
    {
        this.controller.postMicrostreamResumeUpdates();
    }

    @Post(value = PostMicrostreamGc.PATH, consumes = PostMicrostreamGc.CONSUMES, produces = PostMicrostreamGc.PRODUCES)
    public void postMicrostreamGc() throws HttpResponseException
    {
        this.controller.postMicrostreamGc();
    }

    @Get(value = GetMicrostreamGc.PATH, produces = GetMicrostreamGc.PRODUCES)
    public boolean getMicrostreamGc() throws HttpResponseException
    {
        return this.controller.getMicrostreamGc();
    }
}
