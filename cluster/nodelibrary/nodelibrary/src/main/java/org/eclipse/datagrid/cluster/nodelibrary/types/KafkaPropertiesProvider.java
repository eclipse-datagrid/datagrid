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

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT;
import static org.eclipse.serializer.util.X.notNull;

import java.util.Properties;

import org.slf4j.LoggerFactory;

public interface KafkaPropertiesProvider
{
    Properties provide();

    static KafkaPropertiesProvider New(final NodelibraryPropertiesProvider propertiesProvider)
    {
        return new Default(notNull(propertiesProvider));
    }

    final class Default implements KafkaPropertiesProvider
    {
        private final NodelibraryPropertiesProvider properties;

        private Default(final NodelibraryPropertiesProvider propertiesProvider)
        {
            this.properties = propertiesProvider;
        }

        /**
         * Provides kafka properties filled with some default values required by both
         * the consumer and producer.
         */
        @Override
        public Properties provide()
        {
            final var props = new Properties();
            final var logger = LoggerFactory.getLogger(KafkaPropertiesProvider.class);

            props.setProperty(BOOTSTRAP_SERVERS_CONFIG, this.properties.kafkaBootstrapServers());

            if (this.properties.secureKafka())
            {
                logger.info("Setting SASL properties for kafka communication");
                props.setProperty(SECURITY_PROTOCOL_CONFIG, SASL_PLAINTEXT.name);
                props.setProperty(SASL_MECHANISM, "PLAIN");
                props.setProperty(
                    SASL_JAAS_CONFIG,
                    String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        this.properties.kafkaUsername(),
                        this.properties.kafkaPassword()
                    )
                );
            }
            else
            {
                logger.info("Using plain communication with kafka");
            }

            return props;
        }
    }
}
