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

public class BackupProxyClient
{
    // TODO: Implement when needed

    //	private final HttpClient httpClient = HttpClient.newHttpClient();
    //	private final String domain = ClusterEnv.backupProxyServiceUrl();
    //	private final ObjectMapper mapper = new ObjectMapper();
    //
    //	public void uploadBackup(final Path backupFile, final String backupName) throws BackupProxyRequestException
    //	{
    //		try
    //		{
    //			final var res = this.httpClient.send(
    //				HttpRequest.newBuilder()
    //					.PUT(BodyPublishers.ofFile(backupFile))
    //					.uri(this.createUri(backupName))
    //					.header("Content-Type", "application/octet-stream")
    //					.build(),
    //				BodyHandlers.ofString()
    //			);
    //
    //			if (res.statusCode() != 200)
    //			{
    //				throw new BackupProxyRequestException(
    //					"backup upload failed with response " + Objects.toString(res.body())
    //				);
    //			}
    //		}
    //		catch (IOException | InterruptedException | URISyntaxException e)
    //		{
    //			throw new BackupProxyRequestException("failed to upload backup", e);
    //		}
    //	}
    //
    //	public List<BackupListItem> listBackups()
    //	{
    //		try
    //		{
    //			final var res = this.httpClient.send(
    //				HttpRequest.newBuilder().uri(this.createUri("")).build(),
    //				BodyHandlers.ofString()
    //			);
    //
    //			if (res.statusCode() != 200)
    //			{
    //				throw new BackupProxyRequestException("backup download failed with response " + res.body());
    //			}
    //
    //			return this.mapper.readValue(
    //				res.body(),
    //				TypeFactory.defaultInstance().constructParametricType(List.class, BackupListItem.class)
    //			);
    //		}
    //		catch (final IOException | URISyntaxException | InterruptedException e)
    //		{
    //			throw new BackupProxyRequestException("failed to download backup", e);
    //		}
    //	}
    //
    //	public void deleteBackup(final String backupName) throws BackupProxyRequestException
    //	{
    //		try
    //		{
    //			final var res = this.httpClient.send(
    //				HttpRequest.newBuilder().DELETE().uri(this.createUri(backupName)).build(),
    //				BodyHandlers.ofString()
    //			);
    //
    //			if (res.statusCode() != 200)
    //			{
    //				throw new BackupProxyRequestException(
    //					"backup deletion failed with response " + Objects.toString(res.body())
    //				);
    //			}
    //		}
    //		catch (IOException | InterruptedException | URISyntaxException e)
    //		{
    //			throw new BackupProxyRequestException("failed to upload backup", e);
    //		}
    //	}
    //
    //	private URI createUri(final String path) throws URISyntaxException
    //	{
    //		return new URI(
    //			String.format("http://%s/backup/%s", this.domain, URLEncoder.encode(path, StandardCharsets.UTF_8))
    //		);
    //	}
}
