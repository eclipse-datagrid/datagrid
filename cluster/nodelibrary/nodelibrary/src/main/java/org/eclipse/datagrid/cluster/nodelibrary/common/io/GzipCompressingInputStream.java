package org.eclipse.datagrid.cluster.nodelibrary.common.io;

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

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;

/**
 * @author mwyraz Wraps an input stream and compresses it's contents. Similiar
 *         to DeflateInputStream but adds GZIP-header and trailer See
 *         GzipOutputStream for details. LICENSE: Free to use. Contains some
 *         lines from GzipOutputStream, so oracle's license might apply as well!
 */
public class GzipCompressingInputStream extends SequenceInputStream
{
	public GzipCompressingInputStream(final InputStream in)
	{
		this(in, 512);
	}

	public GzipCompressingInputStream(final InputStream in, final int bufferSize)
	{
		super(new StatefullGzipStreamEnumerator(in, bufferSize));
	}

	static enum StreamState
	{
		HEADER, CONTENT, TRAILER
	}

	protected static class StatefullGzipStreamEnumerator implements Enumeration<InputStream>
	{
		protected final InputStream in;
		protected final int bufferSize;
		protected StreamState state;

		public StatefullGzipStreamEnumerator(final InputStream in, final int bufferSize)
		{
			this.in = in;
			this.bufferSize = bufferSize;
			this.state = StreamState.HEADER;
		}

		@Override
		public boolean hasMoreElements()
		{
			return this.state != null;
		}

		@Override
		public InputStream nextElement()
		{
			switch (this.state)
			{
			case HEADER:
				this.state = StreamState.CONTENT;
				return this.createHeaderStream();
			case CONTENT:
				this.state = StreamState.TRAILER;
				return this.createContentStream();
			case TRAILER:
				this.state = null;
				return this.createTrailerStream();
			default:
				throw new IllegalStateException("Unknown StreamState encountered.");
			}
		}

		static final int GZIP_MAGIC = 0x8b1f;
		static final byte[] GZIP_HEADER = new byte[] {
			(byte)GZIP_MAGIC,        // Magic number (short)
			(byte)(GZIP_MAGIC >> 8),  // Magic number (short)
			Deflater.DEFLATED,        // Compression method (CM)
			0,                        // Flags (FLG)
			0,                        // Modification time MTIME (int)
			0,                        // Modification time MTIME (int)
			0,                        // Modification time MTIME (int)
			0,                        // Modification time MTIME (int)
			0,                        // Extra flags (XFLG)
			0                         // Operating system (OS)
		};

		protected InputStream createHeaderStream()
		{
			return new ByteArrayInputStream(GZIP_HEADER);
		}

		protected InternalGzipCompressingInputStream contentStream;

		protected InputStream createContentStream()
		{
			this.contentStream = new InternalGzipCompressingInputStream(new CRC32InputStream(this.in), this.bufferSize);
			return this.contentStream;
		}

		protected InputStream createTrailerStream()
		{
			return new ByteArrayInputStream(this.contentStream.createTrailer());
		}
	}

	/**
	 * Internal stream without header/trailer
	 */
	protected static class CRC32InputStream extends FilterInputStream
	{
		protected CRC32 crc = new CRC32();
		protected long byteCount;

		public CRC32InputStream(final InputStream in)
		{
			super(in);
		}

		@Override
		public int read() throws IOException
		{
			final int val = super.read();
			if (val >= 0)
			{
				this.crc.update(val);
				this.byteCount++;
			}
			return val;
		}

		@Override
		public int read(final byte[] b, final int off, int len) throws IOException
		{
			len = super.read(b, off, len);
			if (len >= 0)
			{
				this.crc.update(b, off, len);
				this.byteCount += len;
			}
			return len;
		}

		public long getCrcValue()
		{
			return this.crc.getValue();
		}

		public long getByteCount()
		{
			return this.byteCount;
		}
	}

	/**
	 * Internal stream without header/trailer
	 */
	protected static class InternalGzipCompressingInputStream extends DeflaterInputStream
	{
		protected final CRC32InputStream crcIn;

		public InternalGzipCompressingInputStream(final CRC32InputStream in, final int bufferSize)
		{
			super(in, new Deflater(Deflater.DEFAULT_COMPRESSION, true), bufferSize);
			this.crcIn = in;
		}

		@Override
		public void close() throws IOException
		{
			if (this.in != null)
			{
				try
				{
					this.def.end();
					this.in.close();
				}
				finally
				{
					this.in = null;
				}
			}
		}

		protected final static int TRAILER_SIZE = 8;

		public byte[] createTrailer()
		{
			final byte[] trailer = new byte[TRAILER_SIZE];
			this.writeTrailer(trailer, 0);
			return trailer;
		}

		/*
		 * Writes GZIP member trailer to a byte array, starting at a given offset.
		 */
		private void writeTrailer(final byte[] buf, final int offset)
		{
			this.writeInt((int)this.crcIn.getCrcValue(), buf, offset); // CRC-32 of uncompr. data
			this.writeInt((int)this.crcIn.getByteCount(), buf, offset + 4); // Number of uncompr. bytes
		}

		/*
		 * Writes integer in Intel byte order to a byte array, starting at a given
		 * offset.
		 */
		private void writeInt(final int i, final byte[] buf, final int offset)
		{
			this.writeShort(i & 0xffff, buf, offset);
			this.writeShort((i >> 16) & 0xffff, buf, offset + 2);
		}

		/*
		 * Writes short integer in Intel byte order to a byte array, starting at a given
		 * offset
		 */
		private void writeShort(final int s, final byte[] buf, final int offset)
		{
			buf[offset] = (byte)(s & 0xff);
			buf[offset + 1] = (byte)((s >> 8) & 0xff);
		}
	}
}
