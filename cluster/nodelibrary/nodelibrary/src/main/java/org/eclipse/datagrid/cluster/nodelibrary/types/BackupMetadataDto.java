package org.eclipse.datagrid.cluster.nodelibrary.types;

public class BackupMetadataDto
{
	private String name;
	private long size;

	public BackupMetadataDto()
	{
	}

	public BackupMetadataDto(final String name, final long size)
	{
		this.name = name;
		this.size = size;
	}

	public String getName()
	{
		return this.name;
	}

	public void setName(final String name)
	{
		this.name = name;
	}

	public long getSize()
	{
		return this.size;
	}

	public void setSize(final long size)
	{
		this.size = size;
	}
}
