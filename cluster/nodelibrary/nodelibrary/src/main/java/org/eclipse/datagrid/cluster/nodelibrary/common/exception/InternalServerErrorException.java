package org.eclipse.datagrid.cluster.nodelibrary.common.exception;

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

public class InternalServerErrorException extends RuntimeException
{
	public InternalServerErrorException()
	{
		super();
	}
	
	public InternalServerErrorException(final String msg)
	{
		super(msg);
	}
	
	public InternalServerErrorException(final Throwable cause)
	{
		super(cause);
	}
	
	public InternalServerErrorException(final String msg, final Throwable cause)
	{
		super(msg, cause);
	}
}
