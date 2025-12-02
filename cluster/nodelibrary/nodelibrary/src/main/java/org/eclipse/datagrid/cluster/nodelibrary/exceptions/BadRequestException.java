package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

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
