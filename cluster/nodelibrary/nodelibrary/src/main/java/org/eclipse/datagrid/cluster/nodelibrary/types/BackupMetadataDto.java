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

public class BackupMetadataDto
{
    private String name;
    private long size;

    public BackupMetadataDto()
    {
    }

    public BackupMetadataDto(final String name, final long size)
    {
        this.name = name;
        this.size = size;
    }

    public String getName()
    {
        return this.name;
    }

    public void setName(final String name)
    {
        this.name = name;
    }

    public long getSize()
    {
        return this.size;
    }

    public void setSize(final long size)
    {
        this.size = size;
    }
}
