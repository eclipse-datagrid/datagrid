package org.eclipse.datagrid.cluster.nodelibrary.common;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary
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

public final class ClusterEnv
{
	public static final String NAD_KEY = "Microstream-NAD";

	public static boolean secureKafka()
	{
		return Boolean.parseBoolean(env("MSCNL_SECURE_KAFKA"));
	}

	public static String kafkaBootstrapServers()
	{
		return env("KAFKA_BOOTSTRAP_SERVERS");
	}

	public static String kafkaTopicName()
	{
		return env("MSCNL_KAFKA_TOPIC_NAME");
	}

	public static boolean isBackupNode()
	{
		return Boolean.parseBoolean(env("IS_BACKUP_NODE"));
	}

	public static Integer keptBackupsCount()
	{
		final String env = env("KEPT_BACKUPS_COUNT");
		return env == null ? null : Integer.parseInt(env);
	}

	public static Integer backupIntervalMinutes()
	{
		final String env = env("BACKUP_INTERVAL_MINUTES");
		return env == null ? null : Integer.parseInt(env);
	}

	public static String backupNodeServiceUrl()
	{
		return env("BACKUP_NODE_SERVICE_URL");
	}

	public static String backupProxyServiceUrl()
	{
		return env("BACKUP_PROXY_SERVICE_URL");
	}

	public static Integer storageLimitCheckerIntervalMinutes()
	{
		final String env = env("STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES");
		return env == null ? null : Integer.parseInt(env);
	}

	public static Double storageLimitCheckerPercent()
	{
		final String env = env("STORAGE_LIMIT_CHECKER_PERCENT");
		return env == null ? null : Double.parseDouble(env);
	}

	public static String storageLimitGB()
	{
		return env("STORAGE_LIMIT_GB");
	}

	public static boolean isMicro()
	{
		return Boolean.parseBoolean(env("MSCNL_IS_MICRO"));
	}

	public static String kafkaUsername()
	{
		return env("MSCNL_KAFKA_USERNAME");
	}

	public static String kafkaPassword()
	{
		return env("MSCNL_KAFKA_PASSWORD");
	}

	public static String myPodName()
	{
		return env("MY_POD_NAME");
	}

	public static String myNamespace()
	{
		return env("MY_NAMESPACE");
	}
	
	public static boolean isProdMode()
	{
		return Boolean.parseBoolean(env("MSCNL_PROD_MODE"));
	}

	private static String env(final String envkey)
	{
		return System.getenv(envkey);
	}

	private ClusterEnv()
	{
	}

}
