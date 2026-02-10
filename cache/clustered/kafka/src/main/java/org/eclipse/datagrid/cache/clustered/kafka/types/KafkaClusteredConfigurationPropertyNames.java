package org.eclipse.datagrid.cache.clustered.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.eclipse.datagrid.cache.clustered.types.ClusteredConfigurationPropertyNames;

public interface KafkaClusteredConfigurationPropertyNames
{
    String PREFIX = ClusteredConfigurationPropertyNames.PREFIX + "kafka.";
    String KAFKA_CONFIG_PREFIX = PREFIX + "config.";
    String KAFKA_PRODUCER_CONFIG_PREFIX = KAFKA_CONFIG_PREFIX + "producer.";
    String KAFKA_CONSUMER_CONFIG_PREFIX = KAFKA_CONFIG_PREFIX + "consumer.";
    String TOPIC = PREFIX + "topic";
}
