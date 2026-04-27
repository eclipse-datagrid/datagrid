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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.store.storage.types.StorageConnection;

import static org.eclipse.serializer.math.XMath.positive;

public interface StorageBackupBackend
{
	List<BackupMetadata> listBackups() throws NodelibraryException;

	void downloadLatestBackup(Path targetRootPath) throws NodelibraryException;

	Optional<MessageInfo> getMessageInfoFromPreviousBackup(int skip) throws NodelibraryException;

	default boolean containsBackups() throws NodelibraryException
	{
		return !this.listBackups().isEmpty();
	}

	default BackupMetadata latestBackup(final boolean ignoreManualSlot) throws NodelibraryException
	{
		return this.listBackups()
			.stream()
			.filter(b -> !ignoreManualSlot || !b.manualSlot())
			.max(Comparator.comparingLong(BackupMetadata::timestamp))
			.orElse(null);
	}

	default Optional<BackupMetadata> getLastBackup(final int skip) throws NodelibraryException
	{
		positive(skip);

		final var backups = this.listBackups();

		if (backups.size() <= skip)
		{
			// no previous storage
			return Optional.empty();
		}

		backups.sort(Comparator.comparingLong(BackupMetadata::timestamp));

		return Optional.of(backups.get(backups.size() - 1 - skip));
	}

	void deleteBackup(BackupMetadata backup) throws NodelibraryException;

	void createAndUploadBackup(StorageConnection connection, final MessageInfo messageInfo, BackupMetadata backup)
		throws NodelibraryException;

	void downloadBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodelibraryException;

	boolean hasUserUploadedStorage() throws NodelibraryException;

	void downloadUserUploadedStorage(Path storageDestinationParentPath) throws NodelibraryException;

	void deleteUserUploadedStorage() throws NodelibraryException;

}
