package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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

import org.eclipse.serializer.collections.Set_long;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataAcceptor;
import org.eclipse.serializer.persistence.types.PersistenceLoader;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.serializer.persistence.types.PersistenceObjectRegistry;
import org.eclipse.serializer.persistence.types.PersistenceRootReference;
import org.eclipse.serializer.persistence.types.PersistenceRoots;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;

public class ObjectMaterializer implements BinaryEntityRawDataAcceptor
{
	private final PersistenceTypeDictionary persistenceTypeDictionary;
	private final PersistenceObjectRegistry objectRegistry;
	private final PersistenceLoader loader;
	private final Set_long oids = Set_long.New();

	public ObjectMaterializer(final PersistenceManager<?> persistenceManager)
	{
		super();

		this.persistenceTypeDictionary = persistenceManager.typeDictionary();
		this.objectRegistry = persistenceManager.objectRegistry();
		this.loader = persistenceManager.createLoader();
	}

	@Override
	public boolean acceptEntityData(final long entityStartAddress, final long dataBoundAddress)
	{
		// check for incomplete entity header
		if (entityStartAddress + Binary.entityHeaderLength() > dataBoundAddress)
		{
			// signal to calling context that entity cannot be processed and header must be reloaded
			return false;
		}

		final PersistenceTypeDefinition ptd = this.persistenceTypeDictionary.lookupTypeById(
			Binary.getEntityTypeIdRawValue(entityStartAddress)
		);
		if (
			PersistenceRoots.class.isAssignableFrom(ptd.type()) || PersistenceRootReference.class.isAssignableFrom(
				ptd.type()
			)
		)
		{
			// don't overwrite local roots
			return true;
		}

		final long oid = Binary.getEntityObjectIdRawValue(entityStartAddress);

		if (this.objectRegistry.containsClearedObject(oid))
		{
			// no need to process known but not loaded objects
			return true;
		}

		/*
		 * Now we know it is either a new object or an existing one which is loaded, so
		 * we need to re-materialize it.
		 *
		 * Adding the oid to a set ensures that each object is only materialized once.
		 * Multiple versions of the object may be in the same data set, mostly when the
		 * replication happens in batch mode.
		 */
		this.oids.add(oid);

		return true;
	}

	public void materialize()
	{
		// Batch-materializes all collected objects in the live graph
		this.loader.collect(obj ->
		{
			// no-op
		}, this.oids);

		// Help the GC
		this.oids.truncate();
	}

}
