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

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.exceptions.PersistenceExceptionTransfer;
import org.eclipse.serializer.persistence.types.PersistenceTarget;

public interface StorageBinaryTargetDistributing extends PersistenceTarget<Binary>
{
	public static StorageBinaryTargetDistributing New(
		final PersistenceTarget<Binary>    delegate   ,
		final StorageBinaryDataDistributor distributor
	)
	{
		return new StorageBinaryTargetDistributing.Default(
			notNull(delegate   ),
			notNull(distributor)
		);
	}
	
	public static class Default implements StorageBinaryTargetDistributing
	{
		private final PersistenceTarget<Binary>    delegate   ;
		private final StorageBinaryDataDistributor distributor;
		

		Default(
			final PersistenceTarget<Binary>    delegate   ,
			final StorageBinaryDataDistributor distributor
		)
		{
			super();
			this.delegate    = delegate   ;
			this.distributor = distributor;
		}

		@Override
		public void write(final Binary data) throws PersistenceExceptionTransfer
		{
			data.iterateChannelChunks(Binary::mark);
			
			this.delegate.write(data);
			
			data.iterateChannelChunks(Binary::reset);
			
			this.distributor.distributeData(data);
		}

		@Override
		public boolean isWritable()
		{
			return this.delegate.isWritable();
		}
		
	}
	
}
