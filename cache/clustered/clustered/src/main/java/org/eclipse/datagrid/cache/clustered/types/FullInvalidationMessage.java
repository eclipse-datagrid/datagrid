package org.eclipse.datagrid.cache.clustered.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import java.util.Objects;

public class FullInvalidationMessage extends InvalidationMessage
{
    private final Object value;

    public FullInvalidationMessage(final String cacheName, final Object key, final Object value)
    {
        super(cacheName, key);
        this.value = value;
    }

    public Object value()
    {
        return value;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (!(o instanceof final FullInvalidationMessage that))
            return false;
        if (!super.equals(o))
            return false;
        return value == that.value;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public String toString()
    {
        return "TimestampsRegionInvalidationMessage{" +
            "super=" + super.toString() + ", " +
            "value=" + value +
            '}';
    }
}
