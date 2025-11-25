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
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRouteConfigurations;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRouteConfigurations.*;
import org.eclipse.serializer.util.X;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@RestController
@RequestMapping(ClusterRestRouteConfigurations.ROOT_PATH)
public class SpringBootClusterController
{
    private final ClusterRestRequestController controller;

    public SpringBootClusterController(final ClusterRestRequestController controller)
    {
        this.controller = controller;
    }

    @GetMapping(value = GetMicrostreamDistributor.PATH, produces = GetMicrostreamDistributor.PRODUCES)
    public boolean getMicrostreamDistributor() throws HttpResponseException
    {
        return this.call(this.controller::getMicrostreamDistributor);
    }

    @PostMapping(
        value = PostMicrostreamActivateDistributorStart.PATH,
        consumes = PostMicrostreamActivateDistributorStart.CONSUMES,
        produces = PostMicrostreamActivateDistributorStart.PRODUCES
    )
    public void postMicrostreamActivateDistributorStart() throws HttpResponseException
    {
        this.call(this.controller::postMicrostreamActivateDistributorStart);
    }

    @PostMapping(
        value = PostMicrostreamActivateDistributorFinish.PATH,
        consumes = PostMicrostreamActivateDistributorFinish.CONSUMES,
        produces = PostMicrostreamActivateDistributorFinish.PRODUCES
    )
    public boolean postMicrostreamActivateDistributorFinish() throws HttpResponseException
    {
        return this.call(this.controller::postMicrostreamActivateDistributorFinish);
    }

    @GetMapping(value = GetMicrostreamHealth.PATH, produces = GetMicrostreamHealth.PRODUCES)
    public void getMicrostreamHealth() throws HttpResponseException
    {
        this.call(this.controller::getMicrostreamHealth);
    }

    @GetMapping(value = GetMicrostreamHealthReady.PATH, produces = GetMicrostreamHealthReady.PRODUCES)
    @Async
    public CompletableFuture<Void> getMicrostreamHealthReady() throws HttpResponseException
    {
        this.call(this.controller::getMicrostreamHealthReady);
        return CompletableFuture.completedFuture(null);
    }

    @GetMapping(value = GetMicrostreamStorageBytes.PATH, produces = GetMicrostreamStorageBytes.PRODUCES)
    @Async
    public CompletableFuture<String> getMicrostreamStorageBytes() throws HttpResponseException
    {
        return CompletableFuture.completedFuture(this.call(this.controller::getMicrostreamStorageBytes));
    }

    @PostMapping(
        value = PostMicrostreamBackup.PATH,
        consumes = PostMicrostreamBackup.CONSUMES,
        produces = PostMicrostreamBackup.PRODUCES
    )
    public void postMicrostreamBackup(@RequestBody final PostMicrostreamBackup.Body body) throws HttpResponseException
    {
        this.call(() -> this.controller.postMicrostreamBackup(body));
    }

    @PostMapping(
        value = PostMicrostreamUpdates.PATH,
        consumes = PostMicrostreamUpdates.CONSUMES,
        produces = PostMicrostreamUpdates.PRODUCES
    )
    public void postMicrostreamUpdates() throws HttpResponseException
    {
        this.call(this.controller::postMicrostreamUpdates);
    }

    @GetMapping(value = GetMicrostreamBackup.PATH, produces = GetMicrostreamBackup.PRODUCES)
    public boolean getMicrostreamBackup() throws HttpResponseException
    {
        return this.call(this.controller::getMicrostreamBackup);
    }

    @GetMapping(value = GetMicrostreamUpdates.PATH, produces = GetMicrostreamUpdates.PRODUCES)
    public boolean getMicrostreamUpdates() throws HttpResponseException
    {
        return this.call(this.controller::getMicrostreamUpdates);
    }

    @PostMapping(
        value = PostMicrostreamResumeUpdates.PATH,
        consumes = PostMicrostreamResumeUpdates.CONSUMES,
        produces = PostMicrostreamResumeUpdates.PRODUCES
    )
    public void postMicrostreamResumeUpdates() throws HttpResponseException
    {
        this.call(this.controller::postMicrostreamResumeUpdates);
    }

    @PostMapping(
        value = PostMicrostreamGc.PATH,
        consumes = PostMicrostreamGc.CONSUMES,
        produces = PostMicrostreamGc.PRODUCES
    )
    public void postMicrostreamGc() throws HttpResponseException
    {
        this.call(this.controller::postMicrostreamGc);
    }

    @GetMapping(value = GetMicrostreamGc.PATH, produces = GetMicrostreamGc.PRODUCES)
    public boolean getMicrostreamGc() throws HttpResponseException
    {
        return this.call(this.controller::getMicrostreamGc);
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
