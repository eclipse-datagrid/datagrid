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

import org.eclipse.store.cache.hibernate.types.ConfigurationPropertyNames;

public interface ClusteredConfigurationPropertyNames
{
    String PREFIX = ConfigurationPropertyNames.PREFIX + "clustered.";
    String SERIALIZATION_TYPES_PROVIDER = PREFIX + "serialization-types-provider";
    String COM_PROVIDER = PREFIX + "com-provider";
}
