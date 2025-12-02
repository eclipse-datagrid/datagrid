package org.eclipse.datagrid.cluster.nodelibrary.types;

public final class BackupMetadata
{
	private final boolean manualSlot;
	private final long timestamp;

	public BackupMetadata(final long timestamp, final boolean manualSlot)
	{
		this.timestamp = timestamp;
		this.manualSlot = manualSlot;
	}

	public long timestamp()
	{
		return this.timestamp;
	}

	public boolean manualSlot()
	{
		return this.manualSlot;
	}
}
