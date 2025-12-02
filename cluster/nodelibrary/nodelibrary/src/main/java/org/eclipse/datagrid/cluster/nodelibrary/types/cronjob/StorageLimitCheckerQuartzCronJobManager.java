package org.eclipse.datagrid.cluster.nodelibrary.types.cronjob;

import org.eclipse.datagrid.cluster.nodelibrary.types.StorageDiskSpaceReader;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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
