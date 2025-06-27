package org.eclipse.datagrid.cluster.nodelibrary.helidon;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary Helidon
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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.exception.NotADistributorException;

@ApplicationScoped
@Provider
public class NotADistributorMapper implements ExceptionMapper<NotADistributorException>
{
	private final Logger logger = LoggerFactory.getLogger(NotADistributorMapper.class);

	@Override
	public Response toResponse(final NotADistributorException exception)
	{
		this.logger.error(
			"Store call has been made on a node that is not the writer! Message: {}",
			exception.getMessage()
		);
		return Response.status(400).header(ClusterEnv.NAD_KEY, Boolean.TRUE.toString()).build();
	}
}
