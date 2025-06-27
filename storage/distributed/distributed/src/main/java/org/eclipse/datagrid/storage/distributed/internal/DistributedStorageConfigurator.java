package org.eclipse.datagrid.storage.distributed.internal;

/*-
 * #%L
 * Eclipse DataGrid Storage Distributed
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 * 
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

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
		if(subject instanceof PersistenceTarget)
		{
			return (T)StorageBinaryTargetDistributing.New(
				(PersistenceTarget<Binary>)subject,
				this.distributor
			);
		}
		if(subject instanceof PersistenceTypeDictionaryExporter)
		{
			return (T)StorageTypeDictionaryExporterDistributing.New(
				(PersistenceTypeDictionaryExporter)subject,
				this.distributor
			);
		}
		
		return subject;
	}
	
	
}
