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
    public boolean getDataGridDistributor() throws HttpResponseException
    {
        return this.controller.getDataGridDistributor();
    }

    @Post(
        value = PostActivateDistributorStart.PATH,
        consumes = PostActivateDistributorStart.CONSUMES,
        produces = PostActivateDistributorStart.PRODUCES
    )
    public void postDataGridActivateDistributorStart() throws HttpResponseException
    {
        this.controller.postDataGridActivateDistributorStart();
    }

    @Post(
        value = PostActivateDistributorFinish.PATH,
        consumes = PostActivateDistributorFinish.CONSUMES,
        produces = PostActivateDistributorFinish.PRODUCES
    )
    public boolean postDataGridActivateDistributorFinish() throws HttpResponseException
    {
        return this.controller.postDataGridActivateDistributorFinish();
    }

    @Get(value = GetHealth.PATH, produces = GetHealth.PRODUCES)
    public void getDataGridHealth() throws HttpResponseException
    {
        this.controller.getDataGridHealth();
    }

    @Get(value = GetHealthReady.PATH, produces = GetHealthReady.PRODUCES)
    @ExecuteOn(TaskExecutors.IO)
    public void getDataGridHealthReady() throws HttpResponseException
    {
        this.controller.getDataGridHealthReady();
    }

    @Get(value = GetStorageBytes.PATH, produces = GetStorageBytes.PRODUCES)
    @ExecuteOn(TaskExecutors.IO)
    public String getDataGridStorageBytes() throws HttpResponseException
    {
        return this.controller.getDataGridStorageBytes();
    }

    @Post(
        value = PostBackup.PATH,
        consumes = PostBackup.CONSUMES,
        produces = PostBackup.PRODUCES
    )
    public void postDataGridBackup(@Body final PostBackup.Body body) throws HttpResponseException
    {
        this.controller.postDataGridBackup(body);
    }

    @Get(value = GetBackup.PATH, produces = GetBackup.PRODUCES)
    public boolean getDataGridBackup() throws HttpResponseException
    {
        return this.controller.getDataGridBackup();
    }

    @Post(
        value = PostUpdates.PATH,
        consumes = PostUpdates.CONSUMES,
        produces = PostUpdates.PRODUCES
    )
    public void postDataGridUpdates() throws HttpResponseException
    {
        this.controller.postDataGridUpdates();
    }

    @Get(value = GetUpdates.PATH, produces = GetUpdates.PRODUCES)
    public boolean getDataGridUpdates() throws HttpResponseException
    {
        return this.controller.getDataGridUpdates();
    }

    @Post(
        value = PostResumeUpdates.PATH,
        consumes = PostResumeUpdates.CONSUMES,
        produces = PostResumeUpdates.PRODUCES
    )
    public void postDataGridResumeUpdates() throws HttpResponseException
    {
        this.controller.postDataGridResumeUpdates();
    }

    @Post(value = PostGc.PATH, consumes = PostGc.CONSUMES, produces = PostGc.PRODUCES)
    public void postDataGridGc() throws HttpResponseException
    {
        this.controller.postDataGridGc();
    }

    @Get(value = GetGc.PATH, produces = GetGc.PRODUCES)
    public boolean getDataGridGc() throws HttpResponseException
    {
        return this.controller.getDataGridGc();
    }
}
