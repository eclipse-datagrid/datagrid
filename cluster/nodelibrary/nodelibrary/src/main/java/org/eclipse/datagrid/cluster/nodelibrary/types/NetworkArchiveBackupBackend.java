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

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface NetworkArchiveBackupBackend extends StorageBackupBackend
{
    static NetworkArchiveBackupBackend New(
        final Path storageExportScratchSpacePath,
        final BackupProxyHttpClient backupProxyHttpClient,
        final StoredMessageInfoManager.Creator storedMessageInfoManagerCreator,
        final MessageInfoParser messageInfoParser
    )
    {
        return new Default(
            notNull(storageExportScratchSpacePath),
            notNull(backupProxyHttpClient),
            notNull(storedMessageInfoManagerCreator),
            notNull(messageInfoParser)
        );
    }

    final class Default implements NetworkArchiveBackupBackend
    {
        private static final Logger LOG = LoggerFactory.getLogger(NetworkArchiveBackupBackend.class);
        private static final String USER_UPLOADED_STORAGE_S3_KEY = "user-uploaded-storage.tar.xz";

        private final Path storageExportScratchSpacePath;
        private final BackupProxyHttpClient http;
        private final StoredMessageInfoManager.Creator messageInfoManagerCreator;
        private final MessageInfoParser messageInfoParser;

        private Default(
            final Path storageExportScratchSpacePath,
            final BackupProxyHttpClient backupProxyHttpClient,
            final StoredMessageInfoManager.Creator storedMessageInfoManagerCreator,
            final MessageInfoParser messageInfoParser
        )
        {
            this.storageExportScratchSpacePath = storageExportScratchSpacePath;
            this.http = backupProxyHttpClient;
            this.messageInfoManagerCreator = storedMessageInfoManagerCreator;
            this.messageInfoParser = messageInfoParser;
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
        public Optional<MessageInfo> getMessageInfoFromPreviousBackup(final int skip) throws NodelibraryException
        {
            LOG.trace("Getting backup metadata info of latest-{}", skip);

            final var previousBackupMetadata = this.getLastBackup(skip).orElse(null);
            if (previousBackupMetadata == null)
            {
                return Optional.empty();
            }

            final Path scratchSpacePath = this.storageExportScratchSpacePath.resolve("messageInfoDl");
            final String archiveFileName = this.toArchiveFileName(previousBackupMetadata);
            final Path archiveFilePath = scratchSpacePath.resolve(archiveFileName);

            try
            {
                Files.createDirectories(scratchSpacePath);
            }
            catch (final IOException e)
            {
                throw new NodelibraryException(e);
            }

            final String offsetFileContent;

            try
            {
                this.http.download(archiveFileName, archiveFilePath);
                offsetFileContent = this.readFileFromArchive(scratchSpacePath.toString(), "offset", archiveFilePath);
            }
            finally
            {
                try
                {
                    this.deleteDirectory(scratchSpacePath);
                }
                catch (final NodelibraryException e)
                {
                    LOG.warn("Failed to clean up message info scratch storage at {}.", scratchSpacePath, e);
                }
            }

            return Optional.of(this.messageInfoParser.parseMessageInfo(offsetFileContent));
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

            final var fs = Storage.DefaultFileSystem();

            this.clearScratchSpace();

            connection.issueFullBackup(fs.ensureDirectory(this.storageExportScratchSpacePath.resolve("storage")));

            try
            {
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
                final String s3Key = archiveFileName;
                this.http.upload(s3Key, archiveFilePath);
            }
            finally
            {
                try
                {
                    this.deleteFile(archiveFilePath);
                }
                catch (final NodelibraryException e)
                {
                    LOG.warn("Failed to clean up exported storage at {}", archiveFilePath, e);
                }
            }
        }

        @Override
        public void downloadBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
            throws NodelibraryException
        {
            final String archiveFileName = this.toArchiveFileName(backup);
            final Path archiveFilePath = storageDestinationParentPath.resolve(archiveFileName);

            final String s3Key = archiveFileName;
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
                        throw new NodelibraryException("Failed to delete file at " + f, e);
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

            final int exitCode;

            try
            {
                // -C parentPath is so the archive contains a 'storage' folder and nothing else
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

        private String readFileFromArchive(
            final String workingDir,
            final String fileToExtract,
            final Path archiveFilePath
        )
            throws NodelibraryException
        {
            LOG.trace("Reading file from storage archive");

            final int exitCode;
            final String fileContent;

            try
            {
                // -C parentPath is so the archive contains a 'storage' folder and nothing else
                final var process = new ProcessBuilder(
                    "tar",
                    "-C",
                    workingDir,
                    "-OJxf",
                    archiveFilePath.toString(),
                    fileToExtract
                ).redirectError(Redirect.INHERIT).start();

                try (final var reader = process.inputReader())
                {
                    fileContent = reader.lines().collect(Collectors.joining("\n"));
                }

                exitCode = process.waitFor();
            }
            catch (final Exception e)
            {
                throw new NodelibraryException("Failed to extract storage", e);
            }

            if (exitCode != 0)
            {
                throw new NodelibraryException("Failed to extract storage. Exit code: " + exitCode);
            }

            return fileContent;
        }

        private String toArchiveFileName(final BackupMetadata backup)
        {
            return backup.timestamp() + (backup.manualSlot() ? ".manual" : "") + ".tar.xz";
        }

    }
}
