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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.meta.NotImplementedYetError;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Comparator;
import java.util.function.Supplier;

import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

public interface StorageBackupManager extends AutoCloseable
{
    void createStorageBackup() throws NodelibraryException;

    @Override
    void close();

    static StorageBackupManager New(
        final StorageConnection storageConnection,
        final File backupsDirFile,
        final int maxBackupCount,
        final Supplier<OffsetInfo> offsetSupplier,
        final StoredOffsetManager.Creator storedOffsetManagerCreator
    )
    {
        return new Default(
            notNull(storageConnection),
            notNull(backupsDirFile),
            positive(maxBackupCount),
            notNull(offsetSupplier),
            notNull(storedOffsetManagerCreator)
        );
    }

    class Default implements StorageBackupManager
    {
        private static final Logger LOG = LoggerFactory.getLogger(StorageBackupManager.class);

        private final StorageConnection storageConnection;
        private final File backupsDirFile;
        private final int maxBackupCount;
        private final Supplier<OffsetInfo> offsetSupplier;
        private final StoredOffsetManager.Creator storedOffsetManagerCreator;

        private Default(
            final StorageConnection storageConnection,
            final File backupsDirFile,
            final int maxBackupCount,
            final Supplier<OffsetInfo> offsetSupplier,
            final StoredOffsetManager.Creator storedOffsetManagerCreator
        )
        {
            this.storageConnection = storageConnection;
            this.backupsDirFile = backupsDirFile;
            this.maxBackupCount = maxBackupCount;
            this.offsetSupplier = offsetSupplier;
            this.storedOffsetManagerCreator = storedOffsetManagerCreator;
        }

        @Override
        public void createStorageBackup() throws NodelibraryException
        {
            LOG.trace("Creating new storage backup");
            final String backupDirName = Long.toString(System.currentTimeMillis());
            final Path backupDirPath = this.backupsDirFile.toPath().resolve(backupDirName + "/");

            final var backupFiles = this.backupsDirFile.listFiles();

            if (backupFiles == null)
            {
                throw new NodelibraryException("Failed to list backup directory at " + this.backupsDirFile.toString());
            }

            if (backupFiles.length >= this.maxBackupCount)
            {
                this.removeOldestBackup(backupFiles);
            }

            final var fs = NioFileSystem.New();
            this.storageConnection.issueFullBackup(fs.ensureDirectory(backupDirPath.resolve("storage")));
            // TODO: Hardcoded offset file name
            try (
                final var offsetWriter = this.storedOffsetManagerCreator.create(
                    fs.ensureFile(backupDirPath.resolve("offset")).tryUseWriting()
                )
            )
            {
                offsetWriter.set(this.offsetSupplier.get());
            }
            final var readyFilePath = backupDirPath.resolve("ready");
            try
            {
                Files.createFile(readyFilePath);
            }
            catch (final IOException e)
            {
                throw new NodelibraryException("Failed to create ready file at " + readyFilePath.toString());
            }
        }

        private void removeOldestBackup(final File[] backupFiles) throws NodelibraryException
        {
            File oldestBackupFile = null;
            FileTime oldestCreationTime = FileTime.from(Instant.now());

            for (final var backupFile : backupFiles)
            {
                final BasicFileAttributes attr;
                try
                {
                    attr = Files.readAttributes(backupFile.toPath(), BasicFileAttributes.class);
                }
                catch (final IOException e)
                {
                    throw new NodelibraryException(
                        "Failed to read attributes for backup directory " + backupFile.toString()
                    );
                }

                if (!attr.isDirectory())
                {
                    throw new NodelibraryException(
                        "Unexpected file encountered in backups directory: " + backupFile.toString()
                    );
                }

                final var creationTime = attr.creationTime();
                if (creationTime.compareTo(oldestCreationTime) < 0)
                {
                    oldestCreationTime = creationTime;
                    oldestBackupFile = backupFile;
                }
            }

            // remove oldest backup
            if (oldestBackupFile != null)
            {
                LOG.debug("Deleting backup at {}", oldestBackupFile);
                try (final var directories = Files.walk(oldestBackupFile.toPath()))
                {
                    directories.sorted(Comparator.reverseOrder()).forEach(f ->
                    {
                        try
                        {
                            Files.delete(f);
                        }
                        catch (final IOException e)
                        {
                            throw new NodelibraryException("Failed to delete file in backup at " + f.toString(), e);
                        }
                    });
                }
                catch (final IOException e)
                {
                    throw new NodelibraryException("Failed to iterate files at " + oldestBackupFile.toString());
                }
            }
        }

        @Override
        public void close()
        {
            // no-op
        }
    }
}
