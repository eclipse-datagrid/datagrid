package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Micronaut
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

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.eclipse.serializer.concurrency.LockedExecutor;

@Factory
public class LockedExecutorFactory
{
    @Singleton
    public LockedExecutor lockedExecutor()
    {
        return LockedExecutor.New();
    }
}
