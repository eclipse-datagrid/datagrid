package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.eclipse.datagrid.cluster.nodelibrary.types.HttpHeader;

public class NotADistributorException extends BadRequestException
{
	public static final String NAD_HEADER_KEY = "StorageNode-NAD";
	public static final String NAD_HEADER_VALUE = Boolean.TRUE.toString();

	public NotADistributorException()
	{
		super();
	}

	public NotADistributorException(final String message)
	{
		super(message);
	}

	public NotADistributorException(final Throwable cause)
	{
		super(cause);
	}

	public NotADistributorException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	public NotADistributorException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

	@Override
	public Collection<HttpHeader> extraHeaders()
	{
		final var headers = new ArrayList<HttpHeader>();
		headers.addAll(super.extraHeaders());
		headers.add(new HttpHeader(NAD_HEADER_KEY, NAD_HEADER_VALUE));
		return Collections.unmodifiableList(headers);
	}
}
