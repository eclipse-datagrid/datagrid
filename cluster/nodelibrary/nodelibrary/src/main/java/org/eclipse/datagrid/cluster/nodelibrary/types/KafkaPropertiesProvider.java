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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface KafkaPropertiesProvider
{
    void init() throws NodelibraryException;

    Properties provide();

    static KafkaPropertiesProvider ConfigDirectory()
    {
        return ConfigDirectory(Defaults.configDirectoryPath());
    }

    static KafkaPropertiesProvider ConfigDirectory(final Path configDirectoryPath)
    {
        return new Default(notNull(configDirectoryPath));
    }

    interface Defaults
    {
        static Path configDirectoryPath()
        {
            return Paths.get("/kafka");
        }
    }

    final class Default implements KafkaPropertiesProvider
    {
        private static final Logger LOG = LoggerFactory.getLogger(KafkaPropertiesProvider.class);
        private final Path configDirectoryPath;
        private Properties properties;

        private Default(final Path configDirectoryPath)
        {
            this.configDirectoryPath = configDirectoryPath;
        }

        @Override
        public void init() throws NodelibraryException
        {
            try (final var configDirectoryStream = Files.list(this.configDirectoryPath))
            {
                configDirectoryStream.filter(Files::isRegularFile).forEach(configFile ->
                {
                    LOG.debug("Reading Kafka config file at {}", configFile);
                    try (final var fileInputStream = new FileInputStream(configFile.toFile()))
                    {
                        this.properties = new Properties();
                        this.properties.load(fileInputStream);
                    }
                    catch (final IOException e)
                    {
                        throw new NodelibraryException(
                            "Failed to load Kafka properties config file at " + configFile,
                            e
                        );
                    }
                });
            }
            catch (final IOException e)
            {
                throw new NodelibraryException("Failed to list Kafka config files at " + this.configDirectoryPath, e);
            }
        }

        @Override
        public Properties provide()
        {
            if (this.properties == null)
            {
                throw new IllegalStateException("Properties file has not been initialized yet");
            }
            final var newProps = new Properties();
            newProps.putAll(this.properties);
            return newProps;
        }
    }
}
