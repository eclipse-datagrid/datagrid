package org.eclipse.datagrid.cluster.nodelibrary.types;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.serializer.persistence.binary.types.Binary;

import static org.eclipse.serializer.util.X.notNull;

public interface ClusterStorageBinaryDataDistributor extends StorageBinaryDataDistributor
{
	void messageIndex(long index);

	long messageIndex();

	void ignoreDistribution(boolean ignore);

	boolean ignoreDistribution();

	static ClusterStorageBinaryDataDistributor Caching(final ClusterStorageBinaryDataDistributor delegate)
	{
		return new Caching(notNull(delegate));
	}

	public static final class Caching implements ClusterStorageBinaryDataDistributor
	{
		private final ClusterStorageBinaryDataDistributor delegate;
		private String typeDictionaryData;

		private Caching(final ClusterStorageBinaryDataDistributor delegate)
		{
			this.delegate = delegate;
		}

		@Override
		public synchronized void distributeData(final Binary data)
		{
			if (this.typeDictionaryData != null)
			{
				this.delegate.distributeTypeDictionary(this.typeDictionaryData);
				this.typeDictionaryData = null;
			}
			this.delegate.distributeData(data);
		}

		@Override
		public synchronized void distributeTypeDictionary(final String typeDictionaryData)
		{
			this.typeDictionaryData = typeDictionaryData;
		}

		@Override
		public void messageIndex(final long index)
		{
			this.delegate.messageIndex(index);
		}

		@Override
		public long messageIndex()
		{
			return this.delegate.messageIndex();
		}

		@Override
		public boolean ignoreDistribution()
		{
			return this.delegate.ignoreDistribution();
		}

		@Override
		public void ignoreDistribution(final boolean ignore)
		{
			this.delegate.ignoreDistribution(ignore);
		}

		@Override
		public void dispose()
		{
			this.delegate.dispose();
		}
	}
}
