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

public final class HttpHeader
{
    private final String key;
    private final String value;

    public HttpHeader(final String key, final String value)
    {
        this.key = key;
        this.value = value;
    }

    public String key()
    {
        return this.key;
    }

    public String value()
    {
        return this.value;
    }
}
