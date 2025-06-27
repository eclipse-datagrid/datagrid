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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.eclipse.serializer.afs.types.AWritableFile;

public class OffsetFileWriter
{
	private Long currentOffset;
	private long lastWrittenOffset = Long.MIN_VALUE;
	private final AWritableFile offsetFile;

	public OffsetFileWriter(final AWritableFile offsetFile)
	{
		this.offsetFile = offsetFile;
	}

	/**
	 * If the last offset has been fully written this new offset will be written to
	 * the offset file. If the previous offset has not been fully written yet, this
	 * method does nothing.
	 */
	public void tryWrite(final long offset)
	{
		if (this.currentOffset != null)
		{
			return;
		}

		this.currentOffset = offset;
		new Thread(this::run, "OffsetFileWriter").start();
	}
	
	public long lastWrittenOffset()
	{
		return this.lastWrittenOffset;
	}

	private void run()
	{
		this.offsetFile.truncate(0);
		this.offsetFile.writeBytes(ByteBuffer.wrap(Long.toString(this.currentOffset).getBytes(StandardCharsets.UTF_8)));
		this.lastWrittenOffset = this.currentOffset;
		this.currentOffset = null;
	}
}
