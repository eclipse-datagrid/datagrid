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

import org.eclipse.serializer.concurrency.XThreads;

@FunctionalInterface
public interface ObjectGraphUpdateHandler
{
	public void objectGraphUpdateAvailable(ObjectGraphUpdater updater);
	
	
	public static ObjectGraphUpdateHandler Synchronized()
	{
		return updater -> XThreads.executeSynchronized(updater::updateObjectGraph);
	}
	
	
}
