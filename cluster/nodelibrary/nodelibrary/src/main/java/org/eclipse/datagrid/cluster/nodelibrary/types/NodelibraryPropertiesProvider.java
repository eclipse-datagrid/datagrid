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

    static NodelibraryPropertiesProvider Env()
    {
        return new Env();
    }

    class Env implements NodelibraryPropertiesProvider
    {
        public static final class EnvKeys
        {
            public static final String SECURE_KAFKA = "MSCNL_SECURE_KAFKA";
            public static final String KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
            public static final String KAFKA_TOPIC_NAME = "MSCNL_KAFKA_TOPIC_NAME";
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
            public static final String IS_MICRO = "MSCNL_IS_MICRO";
            public static final String KAFKA_USERNAME = "MSCNL_KAFKA_USERNAME";
            public static final String KAFKA_PASSWORD = "MSCNL_KAFKA_PASSWORD";
            public static final String MY_POD_NAME = "MY_POD_NAME";
            public static final String MY_NAMESPACE = "MY_NAMESPACE";
            public static final String IS_PROD_MODE = "MSCNL_PROD_MODE";
            public static final String DATA_MERGER_TIMEOUT_MS = "MSCNL_DATA_MERGER_TIMEOUT";

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
