package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

public final class BackupMetadata
{
	private final boolean manualSlot;
	private final long timestamp;

	public BackupMetadata(final long timestamp, final boolean manualSlot)
	{
		this.timestamp = timestamp;
		this.manualSlot = manualSlot;
	}

	public long timestamp()
	{
		return this.timestamp;
	}

	public boolean manualSlot()
	{
		return this.manualSlot;
	}
}
