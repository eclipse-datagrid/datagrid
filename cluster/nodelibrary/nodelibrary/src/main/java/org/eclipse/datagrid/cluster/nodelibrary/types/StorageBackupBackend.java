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

import java.nio.file.Path;
import java.util.List;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.store.storage.types.StorageConnection;


public interface StorageBackupBackend
{
    List<BackupMetadata> listBackups() throws NodelibraryException;

    void downloadLatestBackup(Path targetRootPath) throws NodelibraryException;

    default boolean containsBackups() throws NodelibraryException
    {
        return !this.listBackups().isEmpty();
    }

    default BackupMetadata latestBackup(final boolean ignoreManualSlot) throws NodelibraryException
    {
        return this.listBackups()
            .stream()
            .filter(b -> !ignoreManualSlot || !b.manualSlot())
            .max((a, b) -> Long.compare(a.timestamp(), b.timestamp()))
            .orElse(null);
    }

    void deleteBackup(BackupMetadata backup) throws NodelibraryException;

    void createAndUploadBackup(StorageConnection connection, final MessageInfo messageInfo, BackupMetadata backup)
        throws NodelibraryException;

    void downloadBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodelibraryException;

    boolean hasUserUploadedStorage() throws NodelibraryException;

    void downloadUserUploadedStorage(Path storageDestinationParentPath) throws NodelibraryException;

    void deleteUserUploadedStorage() throws NodelibraryException;

}
