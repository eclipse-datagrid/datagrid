package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

import org.eclipse.serializer.exceptions.BaseException;

public class CronJobException extends BaseException
{
	public CronJobException()
	{
		super();
	}

	public CronJobException(final String message)
	{
		super(message);
	}

	public CronJobException(final Throwable cause)
	{
		super(cause);
	}

	public CronJobException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	public CronJobException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
