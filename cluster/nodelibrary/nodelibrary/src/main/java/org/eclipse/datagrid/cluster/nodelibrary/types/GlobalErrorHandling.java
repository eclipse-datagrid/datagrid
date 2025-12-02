package org.eclipse.datagrid.cluster.nodelibrary.types;

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
