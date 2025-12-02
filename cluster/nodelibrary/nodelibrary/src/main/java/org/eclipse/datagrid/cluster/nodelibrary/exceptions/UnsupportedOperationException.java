package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

public class UnsupportedOperationException extends NodelibraryException
{
	public UnsupportedOperationException()
	{
		super();
	}

	public UnsupportedOperationException(final String message)
	{
		super(message);
	}

	public UnsupportedOperationException(final Throwable cause)
	{
		super(cause);
	}

	public UnsupportedOperationException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	public UnsupportedOperationException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
