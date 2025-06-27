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

public class BackupProxyRequestException extends RuntimeException
{
	public BackupProxyRequestException()
	{
	}

	public BackupProxyRequestException(final String msg)
	{
		super(msg);
	}

	public BackupProxyRequestException(final Throwable cause)
	{
		super(cause);
	}

	public BackupProxyRequestException(final String msg, final Throwable cause)
	{
		super(msg, cause);
	}
}
