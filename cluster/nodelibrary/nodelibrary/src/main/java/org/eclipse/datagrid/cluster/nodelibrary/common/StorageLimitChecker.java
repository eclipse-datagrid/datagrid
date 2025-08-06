package org.eclipse.datagrid.cluster.nodelibrary.common;

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
	public static StorageLimitChecker fromEnv() throws SchedulerException
	{
		final double storageLimitPercent = ClusterEnv.storageLimitCheckerPercent();
		final double storageLimitGb = Double.parseDouble(ClusterEnv.storageLimitGB().replace("G", ""));
		
		final int storageErrorGb = (int)Math.round(storageLimitGb / 100.0 * storageLimitPercent);
		final int intervalMinutes = ClusterEnv.storageLimitCheckerIntervalMinutes();
		
		return new StorageLimitChecker(intervalMinutes, storageErrorGb);
	}
	
	private static final AtomicBoolean LIMIT_REACHED = new AtomicBoolean();
	private static final String STORAGE_ERROR_JOB_DATA_KEY = "storageErrorGb";
	
	private final Logger logger = LoggerFactory.getLogger(StorageLimitChecker.class);
	private final Scheduler scheduler;
	private final int intervalMinutes;
	private final int storageErrorGb;
	
	private StorageLimitChecker(final int intervalMinutes, final int storageErrorGb) throws SchedulerException
	{
		this.intervalMinutes = intervalMinutes;
		this.storageErrorGb = storageErrorGb;
		this.scheduler = StdSchedulerFactory.getDefaultScheduler();
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
			.usingJobData(STORAGE_ERROR_JOB_DATA_KEY, this.storageErrorGb)
			.build();
		
		final Trigger trigger = TriggerBuilder.newTrigger()
			.withSchedule(SimpleScheduleBuilder.repeatMinutelyForever(this.intervalMinutes))
			.usingJobData(STORAGE_ERROR_JOB_DATA_KEY, this.storageErrorGb)
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
	
	public static BigInteger currentStorageDirectorySizeBytes()
	{
		return FileUtils.sizeOfDirectoryAsBigInteger(Paths.get("/storage").toFile());
	}
	
	public static class StorageLimitCheckerJob implements Job
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageLimitCheckerJob.class);
		
		@Override
		public void execute(final JobExecutionContext context) throws JobExecutionException
		{
			LOG.trace("Checking storage size...");
			
			final int storageErrorGb = context.getJobDetail().getJobDataMap().getInt(STORAGE_ERROR_JOB_DATA_KEY);
			final var usedUpStorageBytes = FileUtils.sizeOfDirectoryAsBigInteger(Paths.get("/storage").toFile());
			final int storageSizeGb = usedUpStorageBytes.divide(BigInteger.valueOf(1_000_000_000L)).intValueExact();
			
			LOG.info(
				"ErrorGB: {}, StorageSizeGB: {} (Bytes: {}), LimitReached: {}",
				storageErrorGb,
				storageSizeGb,
				usedUpStorageBytes.toString(),
				storageSizeGb >= storageErrorGb
			);
			
			if (storageSizeGb >= storageErrorGb)
			{
				LOG.warn("Storage limit reached! No more data will be stored!");
				LIMIT_REACHED.set(true);
			}
		}
	}
}
