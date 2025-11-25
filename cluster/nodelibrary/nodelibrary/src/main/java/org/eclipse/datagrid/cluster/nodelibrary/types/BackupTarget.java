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

public enum BackupTarget
{
    SAAS, ONPREM;

    /**
     * Tries to parse the string into the appropriate backup target. If it fails
     * `null` is returned.
     */
    public static BackupTarget parse(final String s)
    {
        if (s == null)
        {
            return null;
        }

        switch (s)
        {
            case "SAAS":
                return SAAS;
            case "ONPREM":
                return ONPREM;
            default:
                return null;
        }
    }
}
