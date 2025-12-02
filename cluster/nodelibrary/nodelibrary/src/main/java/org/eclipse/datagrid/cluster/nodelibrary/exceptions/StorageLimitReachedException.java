package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

public class StorageLimitReachedException extends NodelibraryException
{
	public StorageLimitReachedException()
	{
		super();
	}

	public StorageLimitReachedException(final String message)
	{
		super(message);
	}

	public StorageLimitReachedException(final Throwable cause)
	{
		super(cause);
	}

	public StorageLimitReachedException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	public StorageLimitReachedException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
