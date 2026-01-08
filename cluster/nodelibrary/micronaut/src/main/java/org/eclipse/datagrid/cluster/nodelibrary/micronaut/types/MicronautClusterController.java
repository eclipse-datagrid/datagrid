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

import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.serde.annotation.SerdeImport;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.*;

@Controller(StorageNodeRestRouteConfigurations.ROOT_PATH)
@Introspected(classes = PostBackup.Body.class)
@SerdeImport(PostBackup.Body.class)
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

    @Get(value = GetDistributor.PATH, produces = GetDistributor.PRODUCES)
    public boolean getDistributor() throws HttpResponseException
    {
        return this.controller.getDistributor();
    }

    @Post(
        value = PostActivateDistributorStart.PATH,
        consumes = PostActivateDistributorStart.CONSUMES,
        produces = PostActivateDistributorStart.PRODUCES
    )
    public void postActivateDistributorStart() throws HttpResponseException
    {
        this.controller.postActivateDistributorStart();
    }

    @Post(
        value = PostActivateDistributorFinish.PATH,
        consumes = PostActivateDistributorFinish.CONSUMES,
        produces = PostActivateDistributorFinish.PRODUCES
    )
    public boolean postActivateDistributorFinish() throws HttpResponseException
    {
        return this.controller.postActivateDistributorFinish();
    }

    @Get(value = GetHealth.PATH, produces = GetHealth.PRODUCES)
    public void getHealth() throws HttpResponseException
    {
        this.controller.getHealth();
    }

    @Get(value = GetHealthReady.PATH, produces = GetHealthReady.PRODUCES)
    @ExecuteOn(TaskExecutors.IO)
    public void getHealthReady() throws HttpResponseException
    {
        this.controller.getHealthReady();
    }

    @Get(value = GetStorageBytes.PATH, produces = GetStorageBytes.PRODUCES)
    @ExecuteOn(TaskExecutors.IO)
    public String getStorageBytes() throws HttpResponseException
    {
        return this.controller.getStorageBytes();
    }

    @Post(
        value = PostBackup.PATH,
        consumes = PostBackup.CONSUMES,
        produces = PostBackup.PRODUCES
    )
    public void postBackup(@Body final PostBackup.Body body) throws HttpResponseException
    {
        this.controller.postBackup(body);
    }

    @Get(value = GetBackup.PATH, produces = GetBackup.PRODUCES)
    public boolean getBackup() throws HttpResponseException
    {
        return this.controller.getBackup();
    }

    @Post(
        value = PostUpdates.PATH,
        consumes = PostUpdates.CONSUMES,
        produces = PostUpdates.PRODUCES
    )
    public void postUpdates() throws HttpResponseException
    {
        this.controller.postUpdates();
    }

    @Get(value = GetUpdates.PATH, produces = GetUpdates.PRODUCES)
    public boolean getUpdates() throws HttpResponseException
    {
        return this.controller.getUpdates();
    }

    @Post(
        value = PostResumeUpdates.PATH,
        consumes = PostResumeUpdates.CONSUMES,
        produces = PostResumeUpdates.PRODUCES
    )
    public void postResumeUpdates() throws HttpResponseException
    {
        this.controller.postResumeUpdates();
    }

    @Post(value = PostGc.PATH, consumes = PostGc.CONSUMES, produces = PostGc.PRODUCES)
    public void postGc() throws HttpResponseException
    {
        this.controller.postGc();
    }

    @Get(value = GetGc.PATH, produces = GetGc.PRODUCES)
    public boolean getGc() throws HttpResponseException
    {
        return this.controller.getGc();
    }
}
