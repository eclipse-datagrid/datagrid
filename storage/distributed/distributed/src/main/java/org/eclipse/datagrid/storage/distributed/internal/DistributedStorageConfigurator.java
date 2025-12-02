package org.eclipse.datagrid.storage.distributed.internal;

import static org.eclipse.serializer.util.X.notNull;

import org.eclipse.serializer.functional.InstanceDispatcherLogic;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryTargetDistributing;
import org.eclipse.datagrid.storage.distributed.types.StorageTypeDictionaryExporterDistributing;

public class DistributedStorageConfigurator implements InstanceDispatcherLogic
{
	private final StorageBinaryDataDistributor distributor;

	public DistributedStorageConfigurator(final StorageBinaryDataDistributor distributor)
	{
		super();
		this.distributor = notNull(distributor);
	}

	@SuppressWarnings("unchecked") // type safety ensure by logic
	@Override
	public <T> T apply(final T subject)
	{
		if (subject instanceof PersistenceTarget)
		{
			return (T)StorageBinaryTargetDistributing.New(
				(PersistenceTarget<Binary>)subject,
				this.distributor
			);
		}
		if (subject instanceof PersistenceTypeDictionaryExporter)
		{
			return (T)StorageTypeDictionaryExporterDistributing.New(
				(PersistenceTypeDictionaryExporter)subject,
				this.distributor
			);
		}

		return subject;
	}

}
