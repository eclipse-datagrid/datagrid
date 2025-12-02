package org.eclipse.datagrid.storage.distributed.types;

import static org.eclipse.serializer.util.X.notNull;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;

public interface StorageBinaryDataDistributor extends Disposable
{
	public void distributeData(Binary data);

	public void distributeTypeDictionary(String typeDictionaryData);

	public static StorageBinaryDataDistributor Caching(final StorageBinaryDataDistributor delegate)
	{
		return new StorageBinaryDataDistributor.Caching(
			notNull(delegate)
		);
	}

	/*
	 * Only distribute optional new type dictionary before actual data to minimize
	 * traffic.
	 */
	public static class Caching implements StorageBinaryDataDistributor
	{
		private final StorageBinaryDataDistributor delegate;
		private String typeDictionaryData;

		Caching(final StorageBinaryDataDistributor delegate)
		{
			super();
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
		public void dispose()
		{
			this.delegate.dispose();
		}

	}

}
