package org.eclipse.datagrid.storage.distributed.types;

import static org.eclipse.serializer.util.X.notNull;

import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;

public interface StorageTypeDictionaryExporterDistributing extends PersistenceTypeDictionaryExporter
{
	public static StorageTypeDictionaryExporterDistributing New(
		final PersistenceTypeDictionaryExporter delegate,
		final StorageBinaryDataDistributor distributor
	)
	{
		return new StorageTypeDictionaryExporterDistributing.Default(
			notNull(delegate),
			PersistenceTypeDictionaryAssembler.New(), // use default assembler
			notNull(distributor)
		);
	}

	public static class Default implements StorageTypeDictionaryExporterDistributing
	{
		private final PersistenceTypeDictionaryExporter delegate;
		private final PersistenceTypeDictionaryAssembler assembler;
		private final StorageBinaryDataDistributor distributor;

		Default(
			final PersistenceTypeDictionaryExporter delegate,
			final PersistenceTypeDictionaryAssembler assembler,
			final StorageBinaryDataDistributor distributor
		)
		{
			super();
			this.delegate = delegate;
			this.assembler = assembler;
			this.distributor = distributor;
		}

		@Override
		public void exportTypeDictionary(final PersistenceTypeDictionary typeDictionary)
		{
			this.delegate.exportTypeDictionary(typeDictionary);
			this.distributor.distributeTypeDictionary(
				this.assembler.assemble(typeDictionary)
			);
		}

	}

}
