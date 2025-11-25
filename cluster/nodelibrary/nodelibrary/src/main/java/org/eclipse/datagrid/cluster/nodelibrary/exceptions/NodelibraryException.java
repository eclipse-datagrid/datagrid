package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

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

import org.eclipse.serializer.exceptions.BaseException;

public class NodelibraryException extends BaseException
{
    public NodelibraryException()
    {
        super();
    }

    public NodelibraryException(final String message)
    {
        super(message);
    }

    public NodelibraryException(final Throwable cause)
    {
        super(cause);
    }

    public NodelibraryException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

    public NodelibraryException(
        final String message,
        final Throwable cause,
        final boolean enableSuppression,
        final boolean writableStackTrace
    )
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
