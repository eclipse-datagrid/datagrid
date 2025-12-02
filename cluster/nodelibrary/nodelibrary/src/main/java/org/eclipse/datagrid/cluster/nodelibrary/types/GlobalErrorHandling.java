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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GlobalErrorHandling
{
	private static final Logger LOG = LoggerFactory.getLogger(GlobalErrorHandling.class);

	public static void handleFatalError(final Throwable t)
	{
		try
		{
			LOG.error("Shutting down application due to fatal error", t);
		}
		catch (final Throwable ignored)
		{
			// ignore any failures here
		}

		System.exit(1);
	}

	private GlobalErrorHandling()
	{
	}
}
