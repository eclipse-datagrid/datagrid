package org.eclipse.datagrid.cluster.nodelibrary.springboot;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Spring Boot
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
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.*;
import org.eclipse.serializer.util.X;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@RestController
@RequestMapping(StorageNodeRestRouteConfigurations.ROOT_PATH)
public class SpringBootClusterController
{
    private final ClusterRestRequestController controller;

    public SpringBootClusterController(final ClusterRestRequestController controller)
    {
        this.controller = controller;
    }

    @GetMapping(value = GetDistributor.PATH, produces = GetDistributor.PRODUCES)
    public boolean getDataGridDistributor() throws HttpResponseException
    {
        return this.call(this.controller::getDataGridDistributor);
    }

    @PostMapping(
        value = PostActivateDistributorStart.PATH,
        consumes = PostActivateDistributorStart.CONSUMES,
        produces = PostActivateDistributorStart.PRODUCES
    )
    public void postDataGridActivateDistributorStart() throws HttpResponseException
    {
        this.call(this.controller::postDataGridActivateDistributorStart);
    }

    @PostMapping(
        value = PostActivateDistributorFinish.PATH,
        consumes = PostActivateDistributorFinish.CONSUMES,
        produces = PostActivateDistributorFinish.PRODUCES
    )
    public boolean postDataGridActivateDistributorFinish() throws HttpResponseException
    {
        return this.call(this.controller::postDataGridActivateDistributorFinish);
    }

    @GetMapping(value = GetHealth.PATH, produces = GetHealth.PRODUCES)
    public void getDataGridHealth() throws HttpResponseException
    {
        this.call(this.controller::getDataGridHealth);
    }

    @GetMapping(value = GetHealthReady.PATH, produces = GetHealthReady.PRODUCES)
    @Async
    public CompletableFuture<Void> getDataGridHealthReady() throws HttpResponseException
    {
        this.call(this.controller::getDataGridHealthReady);
        return CompletableFuture.completedFuture(null);
    }

    @GetMapping(value = GetStorageBytes.PATH, produces = GetStorageBytes.PRODUCES)
    @Async
    public CompletableFuture<String> getDataGridStorageBytes() throws HttpResponseException
    {
        return CompletableFuture.completedFuture(this.call(this.controller::getDataGridStorageBytes));
    }

    @PostMapping(
        value = PostBackup.PATH,
        consumes = PostBackup.CONSUMES,
        produces = PostBackup.PRODUCES
    )
    public void postDataGridBackup(@RequestBody final PostBackup.Body body) throws HttpResponseException
    {
        this.call(() -> this.controller.postDataGridBackup(body));
    }

    @PostMapping(
        value = PostUpdates.PATH,
        consumes = PostUpdates.CONSUMES,
        produces = PostUpdates.PRODUCES
    )
    public void postDataGridUpdates() throws HttpResponseException
    {
        this.call(this.controller::postDataGridUpdates);
    }

    @GetMapping(value = GetBackup.PATH, produces = GetBackup.PRODUCES)
    public boolean getDataGridBackup() throws HttpResponseException
    {
        return this.call(this.controller::getDataGridBackup);
    }

    @GetMapping(value = GetUpdates.PATH, produces = GetUpdates.PRODUCES)
    public boolean getDataGridUpdates() throws HttpResponseException
    {
        return this.call(this.controller::getDataGridUpdates);
    }

    @PostMapping(
        value = PostResumeUpdates.PATH,
        consumes = PostResumeUpdates.CONSUMES,
        produces = PostResumeUpdates.PRODUCES
    )
    public void postDataGridResumeUpdates() throws HttpResponseException
    {
        this.call(this.controller::postDataGridResumeUpdates);
    }

    @PostMapping(
        value = PostGc.PATH,
        consumes = PostGc.CONSUMES,
        produces = PostGc.PRODUCES
    )
    public void postDataGridGc() throws HttpResponseException
    {
        this.call(this.controller::postDataGridGc);
    }

    @GetMapping(value = GetGc.PATH, produces = GetGc.PRODUCES)
    public boolean getDataGridGc() throws HttpResponseException
    {
        return this.call(this.controller::getDataGridGc);
    }

    private <T> T call(final Supplier<T> s)
    {
        try
        {
            return s.get();
        }
        catch (final HttpResponseException e)
        {
            throw this.createResponseStatusException(e);
        }
    }

    private void call(final Runnable r)
    {
        try
        {
            r.run();
        }
        catch (final HttpResponseException e)
        {
            throw this.createResponseStatusException(e);
        }
    }

    private ResponseStatusException createResponseStatusException(final HttpResponseException e)
    {
        final var excp = new ResponseStatusException(X.notNull(HttpStatus.resolve(e.statusCode())));
        for (final var header : e.extraHeaders())
        {
            excp.getHeaders().add(header.key(), header.value());
        }
        return excp;
    }
}
