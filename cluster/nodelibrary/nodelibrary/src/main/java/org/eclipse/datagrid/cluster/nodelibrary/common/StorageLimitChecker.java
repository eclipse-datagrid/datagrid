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

import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageLimitChecker
{
	private static final AtomicBoolean LIMIT_REACHED = new AtomicBoolean();
	private static final StorageLimitChecker INST = new StorageLimitChecker();

	public static StorageLimitChecker get()
	{
		return INST;
	}

	private final Logger logger = LoggerFactory.getLogger(StorageLimitChecker.class);
	private final Scheduler scheduler;
	private final int intervalMinutes;
	private final int storageErrorGb;

	private StorageLimitChecker()
	{
		this.intervalMinutes = ClusterEnv.storageLimitCheckerIntervalMinutes();
		final double storageLimitPercent = ClusterEnv.storageLimitCheckerPercent();
		final double storageLimitGb = Double.parseDouble(ClusterEnv.storageLimitGB().replace("G", ""));
		this.storageErrorGb = (int)Math.round(storageLimitGb / 100.0 * storageLimitPercent);

		try
		{
			this.scheduler = StdSchedulerFactory.getDefaultScheduler();
		}
		catch (final SchedulerException e)
		{
			throw new RuntimeException(e);
		}
	}

	public void start() throws SchedulerException
	{
		this.logger.info(
			"Starting storage limit checker. ErrorGB: {}, IntervalMin: {}",
			this.storageErrorGb,
			this.intervalMinutes
		);

		final JobDetail job = JobBuilder.newJob(StorageLimitCheckerJob.class)
			.withIdentity("StorageLimitChecker")
			.usingJobData("storageErrorGb", this.storageErrorGb)
			.build();

		final Trigger trigger = TriggerBuilder.newTrigger()
			.withSchedule(SimpleScheduleBuilder.repeatMinutelyForever(this.intervalMinutes))
			.usingJobData("storageErrorGb", this.storageErrorGb)
			.startNow()
			.build();

		this.scheduler.scheduleJob(job, trigger);
		this.scheduler.start();
	}

	public void stop() throws SchedulerException
	{
		this.scheduler.shutdown();
	}

	public boolean limitReached()
	{
		return LIMIT_REACHED.get();
	}

	public BigInteger currentStorageDirectorySizeBytes()
	{
		return FileUtils.sizeOfDirectoryAsBigInteger(Paths.get("/storage").toFile());
	}

	public static class StorageLimitCheckerJob implements Job
	{
		private final Logger logger = LoggerFactory.getLogger(StorageLimitCheckerJob.class);

		@Override
		public void execute(final JobExecutionContext context) throws JobExecutionException
		{
			this.logger.trace("Checking storage size...");

			final int storageErrorGb = context.getJobDetail().getJobDataMap().getInt("storageErrorGb");
			final var usedUpStorageBytes = FileUtils.sizeOfDirectoryAsBigInteger(Paths.get("/storage").toFile());
			final int storageSizeGb = usedUpStorageBytes.divide(BigInteger.valueOf(1_000_000_000L)).intValueExact();

			this.logger.info(
				"ErrorGB: {}, StorageSizeGB: {} (Bytes: {}), LimitReached: {}",
				storageErrorGb,
				storageSizeGb,
				usedUpStorageBytes.toString(),
				storageSizeGb >= storageErrorGb
			);

			if (storageSizeGb >= storageErrorGb)
			{
				this.logger.warn("Storage limit reached! No more data will be stored!");
				LIMIT_REACHED.set(true);
			}
		}
	}
}
