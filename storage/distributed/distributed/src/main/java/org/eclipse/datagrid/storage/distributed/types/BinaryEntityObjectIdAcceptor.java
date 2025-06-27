package org.eclipse.datagrid.storage.distributed.types;

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

import java.util.function.LongConsumer;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataAcceptor;
import org.eclipse.serializer.persistence.types.PersistenceRootReference;
import org.eclipse.serializer.persistence.types.PersistenceRoots;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.store.storage.types.StorageConnection;

public class BinaryEntityObjectIdAcceptor implements BinaryEntityRawDataAcceptor
{
	private final StorageConnection storage;
	private final LongConsumer      entityConsumer;

	public BinaryEntityObjectIdAcceptor(final StorageConnection storage, final LongConsumer entityConsumer)
	{
		super();
		this.storage        = storage;
		this.entityConsumer = entityConsumer;
	}

	@Override
	public boolean acceptEntityData(final long entityStartAddress, final long dataBoundAddress)
	{
		// check for incomplete entity header
		if(entityStartAddress + Binary.entityHeaderLength() > dataBoundAddress)
		{
			// signal to calling context that entity cannot be processed and header must be reloaded
			return false;
		}
		
		final PersistenceTypeDefinition ptd = this.storage.persistenceManager().typeDictionary().lookupTypeById(
			Binary.getEntityTypeIdRawValue(entityStartAddress)
		);
		if(PersistenceRoots.class.isAssignableFrom(ptd.type())
		|| PersistenceRootReference.class.isAssignableFrom(ptd.type())
		)
		{
			// don't overwrite local roots
			return true;
		}
		
		this.entityConsumer.accept(
			Binary.getEntityObjectIdRawValue(entityStartAddress)
		);
		
		return true;
	}
	
}
