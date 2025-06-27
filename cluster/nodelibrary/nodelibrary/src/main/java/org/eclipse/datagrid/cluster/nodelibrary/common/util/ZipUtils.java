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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

public final class ZipUtils
{
	public static FileSystem createZipFilesystem(final String zipFile) throws IOException, URISyntaxException
	{
		final var fsEnv = new HashMap<String, String>();
		fsEnv.put("create", "true");
		final Path backupFile = Paths.get(zipFile);
		Files.createDirectories(backupFile.getParent());
		return FileSystems.newFileSystem(new URI("jar:file:" + zipFile), fsEnv);
	}

	private ZipUtils()
	{
	}
}
