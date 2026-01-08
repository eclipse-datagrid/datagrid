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
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.eclipse.serializer.util.X.notNull;

public interface NetworkArchiveBackupBackend extends StorageBackupBackend
{
    static NetworkArchiveBackupBackend New(
        final Path storageExportScratchSpacePath,
        final BackupProxyHttpClient backupProxyHttpClient,
        final StoredMessageIndexManager.Creator storedMessageInfoManagerCreator
    )
    {
        return new Default(
            notNull(storageExportScratchSpacePath),
            notNull(backupProxyHttpClient),
            notNull(storedMessageInfoManagerCreator)
        );
    }

    final class Default implements NetworkArchiveBackupBackend
    {
        private static final Logger LOG = LoggerFactory.getLogger(NetworkArchiveBackupBackend.class);
        private static final String USER_UPLOADED_STORAGE_S3_KEY = "user-uploaded-storage.tar.xz";

        private final Path storageExportScratchSpacePath;
        private final BackupProxyHttpClient http;
        private final StoredMessageIndexManager.Creator messageInfoManagerCreator;

        private Default(
            final Path storageExportScratchSpacePath,
            final BackupProxyHttpClient backupProxyHttpClient,
            final StoredMessageIndexManager.Creator storedMessageInfoManagerCreator
        )
        {
            this.storageExportScratchSpacePath = storageExportScratchSpacePath;
            this.http = backupProxyHttpClient;
            this.messageInfoManagerCreator = storedMessageInfoManagerCreator;
        }

        @Override
        public List<BackupMetadata> listBackups() throws NodelibraryException
        {
            try
            {
                return this.http.list()
                    .stream()
                    .filter(m -> !m.getName().equals(USER_UPLOADED_STORAGE_S3_KEY))
                    .map(
                        m -> new BackupMetadata(
                            Long.parseLong(m.getName().substring(0, m.getName().indexOf("."))),
                            m.getName().contains(".manual.")
                        )
                    )
                    .collect(Collectors.toList());
            }
            catch (final NumberFormatException e)
            {
                throw new NodelibraryException("Failed to parse backup metadata list response body", e);
            }
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) throws NodelibraryException
        {
            this.http.delete(this.toArchiveFileName(backup));
        }

        @Override
        public void createAndUploadBackup(
            final StorageConnection connection,
            final MessageInfo messageInfo,
            final BackupMetadata backup
        ) throws NodelibraryException
        {
            final String archiveFileName = this.toArchiveFileName(backup);
            // the exported storage is inside a scratch space volume so we can create the compressed storage there as well
            final Path archiveFilePath = this.storageExportScratchSpacePath.resolve(archiveFileName);
            final String s3Key = archiveFileName;

            final var fs = Storage.DefaultFileSystem();

            this.clearScratchSpace();

            connection.issueFullBackup(fs.ensureDirectory(this.storageExportScratchSpacePath.resolve("storage")));
            // TODO: Hardcoded offset file name
            try (
                final var infoWriter = this.messageInfoManagerCreator.create(
                    fs.ensureFile(this.storageExportScratchSpacePath.resolve("offset")).tryUseWriting()
                )
            )
            {
                infoWriter.set(messageInfo);
            }

            this.compressStorage(this.storageExportScratchSpacePath.toString(), archiveFilePath);
            this.http.upload(s3Key, archiveFilePath);
            this.deleteFile(archiveFilePath);
        }

        @Override
        public void downloadBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
            throws NodelibraryException
        {
            final String archiveFileName = this.toArchiveFileName(backup);
            final String s3Key = archiveFileName;
            final Path archiveFilePath = storageDestinationParentPath.resolve(archiveFileName);

            this.http.download(s3Key, archiveFilePath);
            this.extractStorage(storageDestinationParentPath.toString(), archiveFilePath);
            this.deleteFile(archiveFilePath);
        }

        @Override
        public void downloadLatestBackup(final Path targetRootPath) throws NodelibraryException
        {
            final var backup = this.latestBackup(false);
            if (backup == null)
            {
                throw new NodelibraryException("No backups are available to download");
            }
            this.downloadBackup(targetRootPath, backup);
        }

        @Override
        public void downloadUserUploadedStorage(final Path storageDestinationParentPath) throws NodelibraryException
        {
            final String archiveFileName = USER_UPLOADED_STORAGE_S3_KEY;
            final Path archiveFilePath = storageDestinationParentPath.resolve(archiveFileName);

            this.http.download(archiveFileName, archiveFilePath);
            this.extractStorage(storageDestinationParentPath.toString(), archiveFilePath);
            this.deleteFile(archiveFilePath);
        }

        @Override
        public void deleteUserUploadedStorage() throws NodelibraryException
        {
            this.http.delete(USER_UPLOADED_STORAGE_S3_KEY);
        }

        @Override
        public boolean hasUserUploadedStorage() throws NodelibraryException
        {
            return this.http.list().stream().anyMatch(m -> m.getName().equals(USER_UPLOADED_STORAGE_S3_KEY));
        }

        private void clearScratchSpace()
        {
            final Path storagePath = this.storageExportScratchSpacePath.resolve("storage");
            final Path messageInfoPath = this.storageExportScratchSpacePath.resolve("offset");

            if (Files.exists(storagePath))
            {
                this.deleteDirectory(storagePath);
            }

            if (Files.exists(messageInfoPath))
            {
                this.deleteFile(messageInfoPath);
            }
        }

        private void deleteDirectory(final Path path) throws NodelibraryException
        {
            try (final var directories = Files.walk(path))
            {
                directories.sorted(Comparator.reverseOrder()).forEach(f ->
                {
                    try
                    {
                        Files.delete(f);
                    }
                    catch (final IOException e)
                    {
                        throw new NodelibraryException("Failed to delete file at " + f.toString(), e);
                    }
                });
            }
            catch (final IOException e)
            {
                throw new NodelibraryException("Failed to iterate files at " + path);
            }
        }

        private void compressStorage(final String workingDir, final Path archiveFilePath) throws NodelibraryException
        {
            LOG.trace("Compressing storage");

            // -C parentPath is so the archive contains a 'storage' folder and nothing else
            final int exitCode;

            try
            {
                exitCode = new ProcessBuilder(
                    "tar",
                    "-C",
                    workingDir,
                    "-Jcf",
                    archiveFilePath.toString(),
                    "storage",
                    "offset"
                ).inheritIO().start().waitFor();
            }
            catch (final Exception e)
            {
                if (e instanceof InterruptedException)
                {
                    Thread.currentThread().interrupt();
                }
                throw new NodelibraryException("Failed to compress storage", e);
            }

            if (exitCode != 0)
            {
                throw new NodelibraryException("Failed to compress storage. Exit code: " + exitCode);
            }
        }

        private void deleteFile(final Path path) throws NodelibraryException
        {
            LOG.trace("Deleting file {}", path);
            try
            {
                Files.delete(path);
            }
            catch (final IOException e)
            {
                throw new NodelibraryException("Failed to delete file", e);
            }
        }

        private void extractStorage(final String workingDir, final Path archiveFilePath) throws NodelibraryException
        {
            LOG.trace("Extracting storage archive");

            // -C parentPath is so the archive contains a 'storage' folder and nothing else
            final int exitCode;

            try
            {
                exitCode = new ProcessBuilder("tar", "-C", workingDir, "-Jxf", archiveFilePath.toString()).inheritIO()
                    .start()
                    .waitFor();
            }
            catch (final Exception e)
            {
                if (e instanceof InterruptedException)
                {
                    Thread.currentThread().interrupt();
                }
                throw new NodelibraryException("Failed to extract storage", e);
            }

            if (exitCode != 0)
            {
                throw new NodelibraryException("Failed to extract storage. Exit code: " + exitCode);
            }
        }

        private String toArchiveFileName(final BackupMetadata backup)
        {
            return Long.toString(backup.timestamp()) + (backup.manualSlot() ? ".manual" : "") + ".tar.xz";
        }

    }
}
