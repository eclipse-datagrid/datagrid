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

import static org.eclipse.serializer.util.X.notNull;

import java.nio.ByteBuffer;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.types.PersistenceLoader;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDescription;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.util.X;
import org.eclipse.serializer.util.logging.Logging;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;

public interface StorageBinaryDataMerger extends StorageBinaryDataReceiver
{
	public static StorageBinaryDataMerger New(
		final BinaryPersistenceFoundation<?> foundation              ,
		final StorageConnection              storage                 ,
		final ObjectGraphUpdateHandler       objectGraphUpdateHandler
	)
	{
		return new StorageBinaryDataMerger.Default(
			notNull(foundation              ),
			notNull(storage                 ),
			notNull(objectGraphUpdateHandler)
		);
	}
	
	
	public static class Default implements StorageBinaryDataMerger
	{
		private final static Logger logger = Logging.getLogger(StorageBinaryDataMerger.class);
		
		private final BinaryPersistenceFoundation<?> foundation              ;
		private final StorageConnection              storage                 ;
		private final ObjectGraphUpdateHandler       objectGraphUpdateHandler;
		
		Default(
			final BinaryPersistenceFoundation<?> foundation              ,
			final StorageConnection              storage                 ,
			final ObjectGraphUpdateHandler       objectGraphUpdateHandler
		)
		{
			super();
			this.foundation               = foundation              ;
			this.storage                  = storage                 ;
			this.objectGraphUpdateHandler = objectGraphUpdateHandler;
		}

		@Override
		public synchronized void receiveData(final Binary data)
		{
			logger.debug("Importing data");
			this.storage.importData(X.Enum(data.buffers()));

			final ObjectGraphUpdater updater = () ->
			{
				logger.debug("Updating object graph");
				final PersistenceLoader            loader   = this.storage.persistenceManager().createLoader();
				final BinaryEntityObjectIdAcceptor acceptor = new BinaryEntityObjectIdAcceptor(this.storage, loader::getObject);
				final BinaryEntityRawDataIterator  iterator = BinaryEntityRawDataIterator.New();
				for(final ByteBuffer buffer : data.buffers())
				{
					final long address = XMemory.getDirectByteBufferAddress(buffer);
					iterator.iterateEntityRawData(
			    		address,
			    		address + buffer.limit(),
			    		acceptor
			    	);
				}
			};
			this.objectGraphUpdateHandler.objectGraphUpdateAvailable(updater);
		}
		
		@Override
		public synchronized void receiveTypeDictionary(final String typeDictionaryData)
		{
			final PersistenceTypeDictionary remoteTypeDictionary = BinaryPersistence.Foundation()
				.setClassLoaderProvider    (this.foundation.getClassLoaderProvider()      )
				.setFieldEvaluatorPersister(this.foundation.getFieldEvaluatorPersistable())
				.setTypeDictionaryLoader   (() -> typeDictionaryData                      )
				.getTypeDictionaryProvider ()
				.provideTypeDictionary     ()
			;
			final PersistenceTypeDictionary localTypeDictionary = this.storage.persistenceManager().typeDictionary();
			
			remoteTypeDictionary.iterateAllTypeDefinitions(remoteType ->
			{
				final PersistenceTypeDefinition localType = localTypeDictionary .lookupTypeById(remoteType.typeId());
				if(localType == null)
				{
					logger.debug("New type: " + remoteType.typeName());
					this.foundation.getTypeHandlerManager().ensureTypeHandler(remoteType);
					
				}
				else if(!PersistenceTypeDescription.equalStructure(localType, remoteType))
				{
					throw new RuntimeException(localType + " <> " + remoteType);
				}
			});
		}
		
	}
	
}
