package org.eclipse.datagrid.cluster.nodelibrary.types;

public final class HttpHeader
{
	private final String key;
	private final String value;

	public HttpHeader(final String key, final String value)
	{
		this.key = key;
		this.value = value;
	}

	public String key()
	{
		return this.key;
	}

	public String value()
	{
		return this.value;
	}
}
