package org.eclipse.datagrid.cluster.nodelibrary.common.exception;

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

/**
 * Indicates that storage.store() has been called on a node that is not the
 * distributor!
 */
public class NotADistributorException extends RuntimeException
{
	public NotADistributorException()
	{
	}

	public NotADistributorException(final String msg)
	{
		super(msg);
	}

	public NotADistributorException(final Throwable cause)
	{
		super(cause);
	}

	public NotADistributorException(final String msg, final Throwable cause)
	{
		super(msg, cause);
	}
}
