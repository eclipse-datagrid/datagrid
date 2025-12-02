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

public class BadRequestException extends HttpResponseException
{
	public BadRequestException()
	{
		super();
	}

	public BadRequestException(final String message)
	{
		super(message);
	}

	public BadRequestException(final Throwable cause)
	{
		super(cause);
	}

	public BadRequestException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	public BadRequestException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

	@Override
	public int statusCode()
	{
		return 400;
	}
}
