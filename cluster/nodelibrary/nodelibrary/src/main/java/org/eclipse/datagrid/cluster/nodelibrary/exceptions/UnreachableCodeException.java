package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

public class UnreachableCodeException extends NodelibraryException
{
	public UnreachableCodeException()
	{
		super();
	}

	public UnreachableCodeException(final String message)
	{
		super(message);
	}

	public UnreachableCodeException(final Throwable cause)
	{
		super(cause);
	}

	public UnreachableCodeException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	public UnreachableCodeException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
