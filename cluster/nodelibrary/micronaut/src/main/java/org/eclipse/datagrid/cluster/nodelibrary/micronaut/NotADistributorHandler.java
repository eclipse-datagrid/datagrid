package org.eclipse.datagrid.cluster.nodelibrary.micronaut;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary Micronaut
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.exception.NotADistributorException;

@Singleton
@Produces
@Requires(classes = {
	NotADistributorException.class, ExceptionHandler.class
})
public class NotADistributorHandler implements ExceptionHandler<NotADistributorException, HttpResponse<?>>
{
	private final Logger logger = LoggerFactory.getLogger(NotADistributorHandler.class);

	@Override
	public HttpResponse<?> handle(
		@SuppressWarnings("rawtypes") final HttpRequest request,
		final NotADistributorException exception
	)
	{
		this.logger.error(
			"Store call has been made on a node that is not the writer! Message: {}",
			exception.getMessage()
		);
		return HttpResponse.badRequest().header(ClusterEnv.NAD_KEY, Boolean.TRUE.toString());
	}
}
