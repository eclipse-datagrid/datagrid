package org.eclipse.datagrid.cluster.nodelibrary.springboot;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

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
import org.eclipse.serializer.util.X;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

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
	public boolean getDistributor() throws HttpResponseException
	{
		return this.call(this.controller::getDistributor);
	}

	@PostMapping(
		value = PostActivateDistributorStart.PATH,
		consumes = PostActivateDistributorStart.CONSUMES,
		produces = PostActivateDistributorStart.PRODUCES
	)
	public void postActivateDistributorStart() throws HttpResponseException
	{
		this.call(this.controller::postActivateDistributorStart);
	}

	@PostMapping(
		value = PostActivateDistributorFinish.PATH,
		consumes = PostActivateDistributorFinish.CONSUMES,
		produces = PostActivateDistributorFinish.PRODUCES
	)
	public boolean postActivateDistributorFinish() throws HttpResponseException
	{
		return this.call(this.controller::postActivateDistributorFinish);
	}

	@GetMapping(value = GetHealth.PATH, produces = GetHealth.PRODUCES)
	public void getHealth() throws HttpResponseException
	{
		this.call(this.controller::getHealth);
	}

	@GetMapping(value = GetHealthReady.PATH, produces = GetHealthReady.PRODUCES)
	@Async
	public CompletableFuture<Void> getHealthReady() throws HttpResponseException
	{
		this.call(this.controller::getHealthReady);
		return CompletableFuture.completedFuture(null);
	}

	@GetMapping(value = GetStorageBytes.PATH, produces = GetStorageBytes.PRODUCES)
	@Async
	public CompletableFuture<String> getStorageBytes() throws HttpResponseException
	{
		return CompletableFuture.completedFuture(this.call(this.controller::getStorageBytes));
	}

	@PostMapping(
		value = PostBackup.PATH,
		consumes = PostBackup.CONSUMES,
		produces = PostBackup.PRODUCES
	)
	public void postBackup(@RequestBody final PostBackup.Body body) throws HttpResponseException
	{
		this.call(() -> this.controller.postBackup(body));
	}

	@PostMapping(
		value = PostUpdates.PATH,
		consumes = PostUpdates.CONSUMES,
		produces = PostUpdates.PRODUCES
	)
	public void postUpdates() throws HttpResponseException
	{
		this.call(this.controller::postUpdates);
	}

	@GetMapping(value = GetBackup.PATH, produces = GetBackup.PRODUCES)
	public boolean getBackup() throws HttpResponseException
	{
		return this.call(this.controller::getBackup);
	}

	@GetMapping(value = GetUpdates.PATH, produces = GetUpdates.PRODUCES)
	public boolean getUpdates() throws HttpResponseException
	{
		return this.call(this.controller::getUpdates);
	}

	@PostMapping(
		value = PostResumeUpdates.PATH,
		consumes = PostResumeUpdates.CONSUMES,
		produces = PostResumeUpdates.PRODUCES
	)
	public void postResumeUpdates() throws HttpResponseException
	{
		this.call(this.controller::postResumeUpdates);
	}

	@PostMapping(
		value = PostGc.PATH,
		consumes = PostGc.CONSUMES,
		produces = PostGc.PRODUCES
	)
	public void postGc() throws HttpResponseException
	{
		this.call(this.controller::postGc);
	}

	@GetMapping(value = GetGc.PATH, produces = GetGc.PRODUCES)
	public boolean getGc() throws HttpResponseException
	{
		return this.call(this.controller::getGc);
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
