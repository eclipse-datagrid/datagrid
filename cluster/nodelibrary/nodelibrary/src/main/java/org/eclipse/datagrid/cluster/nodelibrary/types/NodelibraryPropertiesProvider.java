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

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface NodelibraryPropertiesProvider
{
    boolean secureKafka();

    String kafkaBootstrapServers();

    String kafkaTopicName();

    boolean isBackupNode();

    Integer keptBackupsCount();

    BackupTarget backupTarget();

    Integer backupIntervalMinutes();

    String microstreamPath();

    String backupNodeServiceUrl();

    String backupProxyServiceUrl();

    Integer storageLimitCheckerIntervalMinutes();

    Double storageLimitCheckerPercent();

    Integer storageLimitGB();

    boolean isMicro();

    String kafkaUsername();

    String kafkaPassword();

    String myPodName();

    String myNamespace();

    boolean isProdMode();

    Long dataMergerTimeoutMs();

    Long dataMergerCachedDataLimit();

    /**
     * Returns the absolute path to the storage parent directory, containing the backup directory, storage directory and
     * message index file.
     *
     * @return The storage parent path or {@code null} if absent.
     */
    Path storageParentPath() throws InvalidPathException;

    /**
     * Returns the relative path to the storage directory usually relative to the {@link #storageParentPath()}.
     *
     * @return The storage path or {@code null} if absent.
     * @throws InvalidPathException if the {@link Path} could not be converted.
     */
    Path storagePath() throws InvalidPathException;

    /**
     * Returns the relative path to the storage backup directory usually relative to the {@link #storageParentPath()}.
     *
     * @return The storage backup path or {@code null} if absent.
     * @throws InvalidPathException if the {@link Path} could not be converted.
     */
    Path backupPath() throws InvalidPathException;

    /**
     * Returns the relative path to the stored message index used by the storage distribution. This directory is usually
     * relative to the {@link #storageParentPath()}.
     *
     * @return The message index path or {@code null} if absent.
     * @throws InvalidPathException if the {@link Path} could not be converted.
     */
    Path messageIndexPath() throws InvalidPathException;

    /**
     * Returns the relative path to the lucene directory that that should be used for any lucene index. This directory
     * is usually relative to the {@link #storageParentPath()}.
     *
     * @return The lucene directory path or {@code null} if absent.
     * @throws InvalidPathException if the {@link Path} could not be converted.
     */
    Path luceneDirectoryPath() throws InvalidPathException;

    /**
     * Convenience method for resolving the {@link #storagePath()} relative to {@link #storageParentPath()}.
     *
     * @return Returns the resolved {@link Path} or {@code null} if either the parent path or the storage path is
     * {@code null}.
     * @throws InvalidPathException if any {@link Path} could not be converted.
     */
    default Path resolvedStoragePath()
    {
        final Path parent = storageParentPath();
        final Path storage = storagePath();
        if (parent == null || storage == null)
        {
            return null;
        }
        return parent.resolve(storage);
    }

    /**
     * Convenience method for resolving the {@link #backupPath()} relative to {@link #storageParentPath()}.
     *
     * @return Returns the resolved {@link Path} or {@code null} if either the parent path or the storage backup path is
     * {@code null}.
     * @throws InvalidPathException if any {@link Path} could not be converted.
     */
    default Path resolvedBackupPath()
    {
        final Path parent = storageParentPath();
        final Path backup = backupPath();
        if (parent == null || backup == null)
        {
            return null;
        }
        return parent.resolve(backup);
    }

    /**
     * Convenience method for resolving the {@link #messageIndexPath()} relative to {@link #storageParentPath()}.
     *
     * @return Returns the resolved {@link Path} or {@code null} if either the parent path or the message index path is
     * {@code null}.
     * @throws InvalidPathException if any {@link Path} could not be converted.
     */
    default Path resolvedMessageIndexPath()
    {
        final Path parent = storageParentPath();
        final Path index = messageIndexPath();
        if (parent == null || index == null)
        {
            return null;
        }
        return parent.resolve(index);
    }

    /**
     * Convenience method for resolving the {@link #luceneDirectoryPath()} relative to {@link #storageParentPath()}.
     *
     * @return Returns the resolved {@link Path} or {@code null} if either the parent path or the message index path is
     * {@code null}.
     * @throws InvalidPathException if any {@link Path} could not be converted.
     */
    default Path resolvedLuceneDirectoryPath()
    {
        final Path parent = storageParentPath();
        final Path lucene = luceneDirectoryPath();
        if (parent == null || lucene == null)
        {
            return null;
        }
        return parent.resolve(lucene);
    }

    static NodelibraryPropertiesProvider Env()
    {
        return new Env();
    }

    class Env implements NodelibraryPropertiesProvider
    {
        public static final class EnvKeys
        {
            private static final String PREFIX = "MSCNL_";

            public static final String SECURE_KAFKA = PREFIX + "SECURE_KAFKA";
            public static final String KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
            public static final String KAFKA_TOPIC_NAME = PREFIX + "KAFKA_TOPIC_NAME";
            public static final String IS_BACKUP_NODE = "IS_BACKUP_NODE";
            public static final String BACKUP_TARGET = "BACKUP_TARGET";
            public static final String KEPT_BACKUPS_COUNT = "KEPT_BACKUPS_COUNT";
            public static final String BACKUP_INTERVAL_MINUTES = "BACKUP_INTERVAL_MINUTES";
            public static final String MICROSTREAM_PATH = "MICROSTREAM_PATH";
            public static final String BACKUP_NODE_SERVICE_URL = "BACKUP_NODE_SERVICE_URL";
            public static final String BACKUP_PROXY_SERVICE_URL = "BACKUP_PROXY_SERVICE_URL";
            public static final String STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES =
                "STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES";
            public static final String STORAGE_LIMIT_CHECKER_PERCENT = "STORAGE_LIMIT_CHECKER_PERCENT";
            public static final String STORAGE_LIMIT_GB = "STORAGE_LIMIT_GB";
            public static final String IS_MICRO = PREFIX + "IS_MICRO";
            public static final String KAFKA_USERNAME = PREFIX + "KAFKA_USERNAME";
            public static final String KAFKA_PASSWORD = PREFIX + "KAFKA_PASSWORD";
            public static final String MY_POD_NAME = "MY_POD_NAME";
            public static final String MY_NAMESPACE = "MY_NAMESPACE";
            public static final String IS_PROD_MODE = PREFIX + "PROD_MODE";
            public static final String DATA_MERGER_TIMEOUT_MS = PREFIX + "DATA_MERGER_TIMEOUT";
            public static final String DATA_MERGER_LIMIT = PREFIX + "DATA_MERGER_LIMIT";
            public static final String STORAGE_PARENT_PATH = PREFIX + "STORAGE_PARENT_PATH";
            public static final String STORAGE_PATH = PREFIX + "STORAGE_PATH";
            public static final String BACKUP_PATH = PREFIX + "BACKUP_PATH";
            public static final String MESSAGE_INDEX_PATH = PREFIX + "MESSAGE_INDEX_PATH";
            public static final String LUCENE_DIRECTORY_PATH = PREFIX + "LUCENE_DIRECTORY_PATH";

            private EnvKeys()
            {
            }
        }

        @Override
        public boolean secureKafka()
        {
            return this.envBoolean(EnvKeys.SECURE_KAFKA);
        }

        @Override
        public String kafkaBootstrapServers()
        {
            return this.envString(EnvKeys.KAFKA_BOOTSTRAP_SERVERS);
        }

        @Override
        public String kafkaTopicName()
        {
            return this.envString(EnvKeys.KAFKA_TOPIC_NAME);
        }

        @Override
        public boolean isBackupNode()
        {
            return this.envBoolean(EnvKeys.IS_BACKUP_NODE);
        }

        @Override
        public BackupTarget backupTarget()
        {
            return BackupTarget.parse(this.envString(EnvKeys.BACKUP_TARGET));
        }

        @Override
        public Integer keptBackupsCount()
        {
            return this.envInteger(EnvKeys.KEPT_BACKUPS_COUNT);
        }

        @Override
        public Integer backupIntervalMinutes()
        {
            return this.envInteger(EnvKeys.BACKUP_INTERVAL_MINUTES);
        }

        @Override
        public String microstreamPath()
        {
            return this.envString(EnvKeys.MICROSTREAM_PATH);
        }

        @Override
        public String backupNodeServiceUrl()
        {
            return this.envString(EnvKeys.BACKUP_NODE_SERVICE_URL);
        }

        @Override
        public String backupProxyServiceUrl()
        {
            return this.envString(EnvKeys.BACKUP_PROXY_SERVICE_URL);
        }

        @Override
        public Integer storageLimitCheckerIntervalMinutes()
        {
            return this.envInteger(EnvKeys.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES);
        }

        @Override
        public Double storageLimitCheckerPercent()
        {
            return this.envDouble(EnvKeys.STORAGE_LIMIT_CHECKER_PERCENT);
        }

        @Override
        public Integer storageLimitGB()
        {
            // TODO: Change behaviour to set an integer instead of a formatted string like this
            return Integer.parseInt(this.envString(EnvKeys.STORAGE_LIMIT_GB).replace("G", ""));
        }

        @Override
        public boolean isMicro()
        {
            return this.envBoolean(EnvKeys.IS_MICRO);
        }

        @Override
        public String kafkaUsername()
        {
            return this.envString(EnvKeys.KAFKA_USERNAME);
        }

        @Override
        public String kafkaPassword()
        {
            return this.envString(EnvKeys.KAFKA_PASSWORD);
        }

        @Override
        public String myPodName()
        {
            return this.envString(EnvKeys.MY_POD_NAME);
        }

        @Override
        public String myNamespace()
        {
            return this.envString(EnvKeys.MY_NAMESPACE);
        }

        @Override
        public boolean isProdMode()
        {
            return this.envBoolean(EnvKeys.IS_PROD_MODE);
        }

        @Override
        public Long dataMergerTimeoutMs()
        {
            return this.envLong(EnvKeys.DATA_MERGER_TIMEOUT_MS);
        }

        @Override
        public Long dataMergerCachedDataLimit()
        {
            return this.envLong(EnvKeys.DATA_MERGER_LIMIT);
        }

        @Override
        public Path storageParentPath() throws InvalidPathException
        {
            return this.envPath(EnvKeys.STORAGE_PARENT_PATH);
        }

        @Override
        public Path storagePath() throws InvalidPathException
        {
            return this.envPath(EnvKeys.STORAGE_PATH);
        }

        @Override
        public Path backupPath() throws InvalidPathException
        {
            return this.envPath(EnvKeys.BACKUP_PATH);
        }

        @Override
        public Path messageIndexPath() throws InvalidPathException
        {
            return this.envPath(EnvKeys.MESSAGE_INDEX_PATH);
        }

        @Override
        public Path luceneDirectoryPath() throws InvalidPathException
        {
            return this.envPath(EnvKeys.LUCENE_DIRECTORY_PATH);
        }

        private Integer envInteger(final String envkey)
        {
            final String env = this.envString(envkey);
            return env == null ? null : Integer.parseInt(env);
        }

        private Long envLong(final String envkey)
        {
            final String env = this.envString(envkey);
            return env == null ? null : Long.parseLong(env);
        }

        private Double envDouble(final String envkey)
        {
            final String env = this.envString(envkey);
            return env == null ? null : Double.parseDouble(env);
        }

        private Path envPath(final String envkey) throws InvalidPathException
        {
            final String env = envString(envkey);
            return env == null ? null : Paths.get(env);
        }

        private boolean envBoolean(final String envkey)
        {
            return Boolean.parseBoolean(this.envString(envkey));
        }

        private String envString(final String envkey)
        {
            return System.getenv(envkey);
        }
    }
}
