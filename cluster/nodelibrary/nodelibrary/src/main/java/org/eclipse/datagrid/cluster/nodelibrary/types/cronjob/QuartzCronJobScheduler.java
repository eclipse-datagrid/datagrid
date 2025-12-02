package org.eclipse.datagrid.cluster.nodelibrary.types.cronjob;

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


import org.eclipse.datagrid.cluster.nodelibrary.exceptions.CronJobException;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface QuartzCronJobScheduler
{
	void setFactory(QuartzCronJobJobFactory factory) throws CronJobException;

	void schedule(final JobDetail detail, final Trigger trigger) throws CronJobException;

	void start() throws CronJobException;

	void shutdown();

	static QuartzCronJobScheduler New(final Scheduler scheduler)
	{
		return new Default(notNull(scheduler));
	}

	final class Default implements QuartzCronJobScheduler
	{
		private static final Logger LOG = LoggerFactory.getLogger(QuartzCronJobScheduler.class);
		private final Scheduler scheduler;

		private Default(final Scheduler scheduler)
		{
			this.scheduler = scheduler;
		}

		@Override
		public void setFactory(final QuartzCronJobJobFactory factory) throws CronJobException
		{
			try
			{
				this.scheduler.setJobFactory(factory);
			}
			catch (final SchedulerException e)
			{
				throw new CronJobException("Failed to set cron job job factory", e);
			}
		}

		@Override
		public void schedule(final JobDetail detail, final Trigger trigger) throws CronJobException
		{
			try
			{
				this.scheduler.scheduleJob(detail, trigger);
			}
			catch (final SchedulerException e)
			{
				throw new CronJobException("Failed to schedule cron job", e);
			}
		}

		@Override
		public void start() throws CronJobException
		{
			try
			{
				this.scheduler.start();
			}
			catch (final SchedulerException e)
			{
				throw new CronJobException("Failed to start cron job scheduler", e);
			}
		}

		@Override
		public void shutdown()
		{
			LOG.info("Shutting down cron job scheduler");
			try
			{
				this.scheduler.shutdown();
			}
			catch (final SchedulerException e)
			{
				LOG.error("Failed to shutdown cron job scheduler", e);
			}
		}
	}
}
