package org.eclipse.datagrid.cluster.nodelibrary.common.backup;

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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.exception.BackupProxyRequestException;

public class BackupProxyClient
{
	private final HttpClient httpClient = HttpClient.newHttpClient();
	private final String domain = ClusterEnv.backupProxyServiceUrl();
	private final ObjectMapper mapper = new ObjectMapper();

	public void uploadBackup(final Path backupFile, final String backupName) throws BackupProxyRequestException
	{
		try
		{
			final var res = this.httpClient.send(
				HttpRequest.newBuilder()
					.PUT(BodyPublishers.ofFile(backupFile))
					.uri(this.createUri(backupName))
					.header("Content-Type", "application/octet-stream")
					.build(),
				BodyHandlers.ofString()
			);

			if (res.statusCode() != 200)
			{
				throw new BackupProxyRequestException(
					"backup upload failed with response " + Objects.toString(res.body())
				);
			}
		}
		catch (IOException | InterruptedException | URISyntaxException e)
		{
			throw new BackupProxyRequestException("failed to upload backup", e);
		}
	}

	public List<BackupListItem> listBackups()
	{
		try
		{
			final var res = this.httpClient.send(
				HttpRequest.newBuilder().uri(this.createUri("")).build(),
				BodyHandlers.ofString()
			);

			if (res.statusCode() != 200)
			{
				throw new BackupProxyRequestException("backup download failed with response " + res.body());
			}

			return this.mapper.readValue(
				res.body(),
				TypeFactory.defaultInstance().constructParametricType(List.class, BackupListItem.class)
			);
		}
		catch (final IOException | URISyntaxException | InterruptedException e)
		{
			throw new BackupProxyRequestException("failed to download backup", e);
		}
	}

	public void deleteBackup(final String backupName) throws BackupProxyRequestException
	{
		try
		{
			final var res = this.httpClient.send(
				HttpRequest.newBuilder().DELETE().uri(this.createUri(backupName)).build(),
				BodyHandlers.ofString()
			);

			if (res.statusCode() != 200)
			{
				throw new BackupProxyRequestException(
					"backup deletion failed with response " + Objects.toString(res.body())
				);
			}
		}
		catch (IOException | InterruptedException | URISyntaxException e)
		{
			throw new BackupProxyRequestException("failed to upload backup", e);
		}
	}

	private URI createUri(final String path) throws URISyntaxException
	{
		return new URI(
			String.format("http://%s/backup/%s", this.domain, URLEncoder.encode(path, StandardCharsets.UTF_8))
		);
	}
}
