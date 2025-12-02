package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

import org.eclipse.datagrid.cluster.nodelibrary.types.HttpHeader;

import java.util.Collection;
import java.util.Collections;

/**
 * When a subclass of this is thrown it should be mapped to the corresponding
 * http status. (e.g. {@link InternalServerErrorException} should map to a
 * status code 500 response)
 */
public abstract class HttpResponseException extends NodelibraryException
{
	protected HttpResponseException()
	{
		super();
	}

	protected HttpResponseException(final String message)
	{
		super(message);
	}

	protected HttpResponseException(final Throwable cause)
	{
		super(cause);
	}

	protected HttpResponseException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	protected HttpResponseException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public abstract int statusCode();

	public Collection<HttpHeader> extraHeaders()
	{
		return Collections.emptyList();
	}
}
