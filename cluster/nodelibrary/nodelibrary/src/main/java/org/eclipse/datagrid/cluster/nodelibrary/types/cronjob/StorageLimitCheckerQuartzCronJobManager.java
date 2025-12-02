package org.eclipse.datagrid.cluster.nodelibrary.types.cronjob;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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

import org.eclipse.datagrid.cluster.nodelibrary.types.StorageDiskSpaceReader;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.math.XMath.positive;

import static org.eclipse.serializer.util.X.notNull;

public interface StorageLimitCheckerQuartzCronJobManager extends QuartzCronJobManager
{
	boolean limitReached();

	static StorageLimitCheckerQuartzCronJobManager New(
		final int storageSizeLimitGb,
		final StorageDiskSpaceReader diskSpaceReader
	)
	{
		return new Default(positive(storageSizeLimitGb), notNull(diskSpaceReader));
	}

	final class Default implements StorageLimitCheckerQuartzCronJobManager
	{
		private final AtomicBoolean limitReached = new AtomicBoolean(false);
		private final int storageSizeLimitGb;
		private final StorageDiskSpaceReader diskSpaceReader;

		private Default(final int storageSizeLimitGb, final StorageDiskSpaceReader diskSpaceReader)
		{
			this.storageSizeLimitGb = storageSizeLimitGb;
			this.diskSpaceReader = diskSpaceReader;
		}

		@Override
		public Job create()
		{
			return new StorageLimitCheckerQuartzCronJob(
				this.limitReached::set,
				this.storageSizeLimitGb,
				this.diskSpaceReader
			);
		}

		@Override
		public boolean limitReached()
		{
			return this.limitReached.get();
		}
	}

	@DisallowConcurrentExecution
	final class StorageLimitCheckerQuartzCronJob implements Job
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageLimitCheckerQuartzCronJob.class);
		private final Consumer<Boolean> limitReachedCallback;
		private final int storageSizeLimitGb;
		private final StorageDiskSpaceReader diskSpaceReader;

		private StorageLimitCheckerQuartzCronJob(
			final Consumer<Boolean> limitReachedCallback,
			final int storageSizeLimitGb,
			final StorageDiskSpaceReader diskSpaceReader
		)
		{
			this.limitReachedCallback = limitReachedCallback;
			this.storageSizeLimitGb = storageSizeLimitGb;
			this.diskSpaceReader = diskSpaceReader;
		}

		@Override
		public void execute(final JobExecutionContext context) throws JobExecutionException
		{
			LOG.trace("Executing storage limit checker cron job");
			final long storageSizeBytes = this.diskSpaceReader.readUsedDiskSpaceBytes();
			final int storageSizeGb = (int)(storageSizeBytes / 1_000_000_000L);
			LOG.info("Storage Size: {}gb/{}gb ({} bytes)", storageSizeGb, this.storageSizeLimitGb, storageSizeBytes);
			if (storageSizeGb >= this.storageSizeLimitGb)
			{
				LOG.warn("Storage limit reached! No more data will be stored!");
				this.limitReachedCallback.accept(true);
			}
		}
	}
}
