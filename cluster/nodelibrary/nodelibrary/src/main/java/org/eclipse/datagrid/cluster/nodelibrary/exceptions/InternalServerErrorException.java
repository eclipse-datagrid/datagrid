package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

public class InternalServerErrorException extends HttpResponseException
{
	public InternalServerErrorException()
	{
		super();
	}

	public InternalServerErrorException(final String message)
	{
		super(message);
	}

	public InternalServerErrorException(final Throwable cause)
	{
		super(cause);
	}

	public InternalServerErrorException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	public InternalServerErrorException(
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
		return 500;
	}
}
