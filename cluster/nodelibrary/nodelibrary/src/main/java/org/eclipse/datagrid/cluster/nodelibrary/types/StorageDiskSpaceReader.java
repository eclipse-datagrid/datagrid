package org.eclipse.datagrid.cluster.nodelibrary.types;

import org.eclipse.serializer.afs.types.ADirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface StorageDiskSpaceReader
{
	long readUsedDiskSpaceBytes();

	static StorageDiskSpaceReader New(final ADirectory storageDir)
	{
		return new Default(notNull(storageDir));
	}

	class Default implements StorageDiskSpaceReader
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageDiskSpaceReader.class);
		private final ADirectory storageDir;

		private long lastLog = System.currentTimeMillis();

		private Default(final ADirectory storageDir)
		{
			this.storageDir = storageDir;
		}

		@Override
		public long readUsedDiskSpaceBytes()
		{
			final long sizeBytes = this.totalSize(this.storageDir);
			if (LOG.isTraceEnabled() && System.currentTimeMillis() - this.lastLog > 600_000L)
			{
				LOG.trace("Read current storage disk space ({})", sizeBytes);
				this.lastLog = System.currentTimeMillis();
			}
			return sizeBytes;
		}

		private long totalSize(final ADirectory dir)
		{
			final long[] total = {
				0L
			};
			dir.iterateFiles(f ->
			{
				try
				{
					total[0] += f.size();
				}
				catch (final RuntimeException e)
				{
					// the running storage might delete some files
					// TODO: Better way would be to queue a storage task
				}
			});
			dir.iterateDirectories(d -> total[0] += this.totalSize(d));
			return total[0];
		}
	}
}
