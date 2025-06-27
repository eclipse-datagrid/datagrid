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

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT;

import java.util.Properties;

import org.slf4j.LoggerFactory;

public final class KafkaPropertiesProvider
{
	/**
	 * Provides kafka properties filled with some default values required by both
	 * the consumer and producer.
	 */
	public static Properties provide()
	{
		final var props = new Properties();
		final var logger = LoggerFactory.getLogger(KafkaPropertiesProvider.class);

		props.setProperty(BOOTSTRAP_SERVERS_CONFIG, ClusterEnv.kafkaBootstrapServers());

		if (ClusterEnv.secureKafka())
		{
			logger.info("Setting SASL properties for kafka communication");
			props.setProperty(SECURITY_PROTOCOL_CONFIG, SASL_PLAINTEXT.name);
			props.setProperty(SASL_MECHANISM, "PLAIN");
			props.setProperty(
				SASL_JAAS_CONFIG,
				"org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"" + ClusterEnv
					.kafkaUsername() + "\" password=\"" + ClusterEnv.kafkaPassword() + "\";"
			);
		}
		else
		{
			logger.info("Using plain communication with kafka");
		}

		return props;
	}

	private KafkaPropertiesProvider()
	{
	}
}
