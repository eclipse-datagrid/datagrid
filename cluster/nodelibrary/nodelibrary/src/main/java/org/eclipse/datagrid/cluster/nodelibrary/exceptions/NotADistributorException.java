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

import org.eclipse.datagrid.cluster.nodelibrary.types.HttpHeader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class NotADistributorException extends BadRequestException
{
    public static final String NAD_HEADER_KEY = "StorageNode-NAD";
    public static final String NAD_HEADER_VALUE = Boolean.TRUE.toString();

    public NotADistributorException()
    {
        super();
    }

    public NotADistributorException(final String message)
    {
        super(message);
    }

    public NotADistributorException(final Throwable cause)
    {
        super(cause);
    }

    public NotADistributorException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

    public NotADistributorException(
        final String message,
        final Throwable cause,
        final boolean enableSuppression,
        final boolean writableStackTrace
    )
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    @Override
    public Collection<HttpHeader> extraHeaders()
    {
        final var headers = new ArrayList<HttpHeader>();
        headers.addAll(super.extraHeaders());
        headers.add(new HttpHeader(NAD_HEADER_KEY, NAD_HEADER_VALUE));
        return Collections.unmodifiableList(headers);
    }
}
