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

public class InvalidationMessage
{
    private final String cacheName;
    private final Object key;

    public InvalidationMessage(String cacheName, Object key)
    {
        this.cacheName = cacheName;
        this.key = key;
    }

    public String cacheName()
    {
        return cacheName;
    }

    public Object key()
    {
        return key;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        var that = (InvalidationMessage)obj;
        return Objects.equals(this.cacheName, that.cacheName) &&
            Objects.equals(this.key, that.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cacheName, key);
    }

    @Override
    public String toString()
    {
        return "InvalidationMessage[" +
            "cacheName=" + cacheName + ", " +
            "key=" + key + ']';
    }
}
