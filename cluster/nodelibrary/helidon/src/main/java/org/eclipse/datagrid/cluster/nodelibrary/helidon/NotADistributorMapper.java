package org.eclipse.datagrid.cluster.nodelibrary.helidon;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;

@ApplicationScoped
@Provider
public class NotADistributorMapper implements ExceptionMapper<HttpResponseException>
{
	@Override
	public Response toResponse(final HttpResponseException e)
	{
		final var response = Response.status(e.statusCode());
		for (final var header : e.extraHeaders())
		{
			response.header(header.key(), header.value());
		}
		return response.build();
	}
}
