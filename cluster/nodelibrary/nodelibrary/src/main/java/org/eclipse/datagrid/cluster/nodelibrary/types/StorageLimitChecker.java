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

import static org.eclipse.serializer.util.X.notNull;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.math.XMath;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface StorageLimitChecker
{
    void start() throws NodelibraryException;

    void stop() throws NodelibraryException;

    boolean limitReached();

    static StorageLimitChecker New(
        final int intervalMinutes,
        final int storageErrorGb,
        final Scheduler scheduler,
        final StorageDiskSpaceReader diskSpaceReader
    )
    {
        return new Default(
            XMath.positive(intervalMinutes),
            XMath.positive(storageErrorGb),
            notNull(scheduler),
            notNull(diskSpaceReader)
        );
    }

    class Default implements StorageLimitChecker
    {
        private static final Logger LOG = LoggerFactory.getLogger(StorageLimitChecker.class);
        private static final AtomicBoolean LIMIT_REACHED = new AtomicBoolean();
        private static final String STORAGE_ERROR_JOB_DATA_KEY = "storageErrorGb";

        private final Scheduler scheduler;
        private final int intervalMinutes;
        private final int storageErrorGb;
        // TODO: How to not do this workaround? Only works because we have a single disk reader
        private static StorageDiskSpaceReader diskSpaceReader;

        private Default(
            final int intervalMinutes,
            final int storageErrorGb,
            final Scheduler scheduler,
            final StorageDiskSpaceReader diskSpaceReader
        )
        {
            this.intervalMinutes = intervalMinutes;
            this.storageErrorGb = storageErrorGb;
            this.scheduler = scheduler;
            Default.diskSpaceReader = diskSpaceReader;
        }

        @Override
        public void start() throws NodelibraryException
        {
            final JobDetail job = JobBuilder.newJob(StorageLimitCheckerJob.class)
                .withIdentity("StorageLimitChecker")
                .usingJobData(STORAGE_ERROR_JOB_DATA_KEY, this.storageErrorGb)
                .build();

            final Trigger trigger = TriggerBuilder.newTrigger()
                .withSchedule(SimpleScheduleBuilder.repeatMinutelyForever(this.intervalMinutes))
                .usingJobData(STORAGE_ERROR_JOB_DATA_KEY, this.storageErrorGb)
                .startNow()
                .build();

            LOG.trace("Scheduling storage limit checker job.");

            try
            {
                this.scheduler.scheduleJob(job, trigger);
                this.scheduler.start();
            }
            catch (final SchedulerException e)
            {
                throw new NodelibraryException("Failed to start scheduler", e);
            }
        }

        @Override
        public void stop() throws NodelibraryException
        {
            LOG.trace("Stopping storage limit checker");
            try
            {
                this.scheduler.shutdown();
            }
            catch (final SchedulerException e)
            {
                throw new NodelibraryException("Failed to shutdown scheduler", e);
            }
        }

        @Override
        public boolean limitReached()
        {
            return LIMIT_REACHED.get();
        }

        public static class StorageLimitCheckerJob implements Job
        {
            private static final Logger LOG = LoggerFactory.getLogger(StorageLimitCheckerJob.class);

            @Override
            public void execute(final JobExecutionContext context) throws JobExecutionException
            {
                LOG.trace("Executing storage limit checker job...");

                final int storageErrorGb = context.getJobDetail().getJobDataMap().getInt(STORAGE_ERROR_JOB_DATA_KEY);
                final var usedUpStorageBytes = BigInteger.valueOf(diskSpaceReader.readUsedDiskSpaceBytes());
                final int storageSizeGb = usedUpStorageBytes.divide(BigInteger.valueOf(1_000_000_000L)).intValueExact();

                LOG.info(
                    "ErrorGB: {}, StorageSizeGB: {} (Bytes: {}), LimitReached: {}",
                    storageErrorGb,
                    storageSizeGb,
                    usedUpStorageBytes,
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
}
