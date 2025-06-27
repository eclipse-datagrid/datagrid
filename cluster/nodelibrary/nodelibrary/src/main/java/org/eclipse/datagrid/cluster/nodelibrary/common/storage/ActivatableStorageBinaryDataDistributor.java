package org.eclipse.datagrid.cluster.nodelibrary.common.storage;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary
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

import org.eclipse.serializer.persistence.binary.types.Binary;

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;

public class ActivatableStorageBinaryDataDistributor implements StorageBinaryDataDistributor
{
	private final StorageBinaryDataDistributor distributor;
	private boolean active = false;

	public ActivatableStorageBinaryDataDistributor(final StorageBinaryDataDistributor distributor)
	{
		this.distributor = distributor;
	}

	public boolean isActive()
	{
		return this.active;
	}

	public void setActive(final boolean active)
	{
		this.active = active;
	}

	@Override
	public void dispose()
	{
		this.distributor.dispose();
	}

	@Override
	public void distributeData(final Binary data)
	{
		if (this.active)
		{
			this.distributor.distributeData(data);
		}
	}

	@Override
	public void distributeTypeDictionary(final String typeDictionaryData)
	{
		if (this.active)
		{
			this.distributor.distributeTypeDictionary(typeDictionaryData);
		}
	}
}
