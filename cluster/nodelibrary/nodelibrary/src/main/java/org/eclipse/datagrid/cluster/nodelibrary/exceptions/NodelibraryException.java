package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

import org.eclipse.serializer.exceptions.BaseException;

public class NodelibraryException extends BaseException
{
	public NodelibraryException()
	{
		super();
	}

	public NodelibraryException(final String message)
	{
		super(message);
	}

	public NodelibraryException(final Throwable cause)
	{
		super(cause);
	}

	public NodelibraryException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	public NodelibraryException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
