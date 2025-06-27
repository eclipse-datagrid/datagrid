package org.eclipse.datagrid.cluster.nodelibrary.common.util;

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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import org.eclipse.datagrid.cluster.nodelibrary.common.exception.ArchiveException;

public final class GzipUtils
{
	private GzipUtils()
	{
	}

	public static void compressTarGzip(final Path inputPath, final Path outputFile) throws ArchiveException
	{
		try (
			OutputStream outputStream = Files.newOutputStream(outputFile);
			GzipCompressorOutputStream gzipOut = new GzipCompressorOutputStream(outputStream);
			TarArchiveOutputStream tarOut = new TarArchiveOutputStream(gzipOut);
			final Stream<Path> walk = Files.walk(inputPath)
		)
		{
			walk.filter(Files::isRegularFile).forEach(p ->
			{
				try
				{
					final TarArchiveEntry entry = new TarArchiveEntry(p.toFile());
					tarOut.putArchiveEntry(entry);
					Files.copy(p, tarOut);
					tarOut.closeArchiveEntry();
				}
				catch (final IOException e)
				{
					throw new ArchiveException("failed to write archive entry", e);
				}
			});
		}
		catch (final IOException e)
		{
			throw new ArchiveException("failed to open archive stream", e);
		}
	}

	public static void extractTarGZ(final InputStream in) throws IOException
	{
		try (TarArchiveInputStream tarIn = new TarArchiveInputStream(new GzipCompressorInputStream(in)))
		{
			final int bufferSize = 1024;
			TarArchiveEntry entry;

			while ((entry = (TarArchiveEntry)tarIn.getNextEntry()) != null)
			{
				/** If the entry is a directory, create the directory. **/
				if (entry.isDirectory())
				{
					final File f = Paths.get("/storage", entry.getName()).toFile();
					final boolean created = f.mkdirs();
					if (!created)
					{
						System.out.printf(
							"Unable to create directory '%s', during extraction of archive contents.\n",
							f.getAbsolutePath()
						);
					}
				}
				else
				{
					int count;
					final byte data[] = new byte[bufferSize];

					final String parent = new File(entry.getName().replaceFirst("storage", "/storage")).getParent();
					if (parent != null)
					{
						new File(parent).mkdirs();
					}

					final FileOutputStream fos = new FileOutputStream(
						entry.getName().replaceFirst("storage", "/storage"),
						false
					);
					try (BufferedOutputStream dest = new BufferedOutputStream(fos, bufferSize))
					{
						while ((count = tarIn.read(data, 0, bufferSize)) != -1)
						{
							dest.write(data, 0, count);
						}
					}
				}
			}
		}
	}
}
