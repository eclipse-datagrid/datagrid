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


import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.eclipse.serializer.util.X.notNull;

public interface BackupProxyHttpClient
{
	void upload(final String s3Key, final Path filePath) throws NodelibraryException;

	void delete(final String s3Key) throws NodelibraryException;

	Path download(final String s3Key, final Path destinationFilePath) throws NodelibraryException;

	List<BackupMetadataDto> list() throws NodelibraryException;

	static BackupProxyHttpClient New(final URI baseUri)
	{
		return new Default(notNull(baseUri));
	}

	class Default implements BackupProxyHttpClient
	{
		private static final Logger LOG = LoggerFactory.getLogger(BackupProxyHttpClient.class);

		private final HttpClient http = HttpClient.newHttpClient();
		private final ObjectMapper mapper = new ObjectMapper();

		private final URI baseUri;

		private Default(final URI baseUri)
		{
			this.baseUri = baseUri;
		}

		@Override
		public void upload(final String s3Key, final Path filePath) throws NodelibraryException
		{
			LOG.trace("Uploading compressed storage archive");

			final HttpResponse<Void> res;

			try
			{
				res = this.http.send(
					HttpRequest.newBuilder()
						.PUT(BodyPublishers.ofFile(filePath))
						.uri(this.baseUri.resolve(s3Key))
						.build(),
					BodyHandlers.discarding()
				);
			}
			catch (final Exception e)
			{
				if (e instanceof InterruptedException)
				{
					Thread.currentThread().interrupt();
				}
				throw new NodelibraryException("Failed to send upload storage request", e);
			}

			this.validateOkResponse(res.statusCode());
		}

		@Override
		public void delete(final String s3Key) throws NodelibraryException
		{
			final HttpResponse<Void> res;

			try
			{
				res = this.http.send(
					HttpRequest.newBuilder().DELETE().uri(this.baseUri.resolve(s3Key)).build(),
					BodyHandlers.discarding()
				);
			}
			catch (final Exception e)
			{
				if (e instanceof InterruptedException)
				{
					Thread.currentThread().interrupt();
				}
				throw new NodelibraryException("Failed to delete storage archive from s3", e);
			}

			this.validateOkResponse(res.statusCode());
		}

		@Override
		public Path download(final String s3Key, final Path destinationFilePath) throws NodelibraryException
		{
			LOG.trace("Downloading storage archive");

			final HttpResponse<Path> res;

			try
			{
				res = this.http.send(
					HttpRequest.newBuilder().GET().uri(this.baseUri.resolve(s3Key)).build(),
					BodyHandlers.ofFile(destinationFilePath)
				);
			}
			catch (final Exception e)
			{
				if (e instanceof InterruptedException)
				{
					Thread.currentThread().interrupt();
				}
				throw new NodelibraryException("Failed to send download storage request", e);
			}

			this.validateOkResponse(res.statusCode());

			return res.body();
		}

		@Override
		public List<BackupMetadataDto> list() throws NodelibraryException
		{
			final HttpResponse<String> res;

			try
			{
				res = this.http.send(HttpRequest.newBuilder().uri(this.baseUri).build(), BodyHandlers.ofString());
			}
			catch (final Exception e)
			{
				if (e instanceof InterruptedException)
				{
					Thread.currentThread().interrupt();
				}
				throw new NodelibraryException("Failed to send list backups request", e);
			}

			if (res.statusCode() != 200)
			{
				throw new NodelibraryException(
					"Unexpected response from backup proxy. Status Code: " + res.statusCode()
				);
			}

			try
			{
				return this.mapper.readValue(
					res.body(),
					this.mapper.getTypeFactory().constructCollectionType(ArrayList.class, BackupMetadataDto.class)
				);
			}
			catch (final JacksonException e)
			{
				throw new NodelibraryException("Failed to parse backup metadata list response body", e);
			}
		}

		private void validateOkResponse(final int statusCode)
		{
			if (statusCode / 100 != 2)
			{
				throw new NodelibraryException("Unexpected response from backup proxy. Status Code: " + statusCode);
			}
		}
	}
}
