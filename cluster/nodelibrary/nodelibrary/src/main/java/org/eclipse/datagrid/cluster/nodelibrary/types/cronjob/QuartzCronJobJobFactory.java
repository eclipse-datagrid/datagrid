package org.eclipse.datagrid.cluster.nodelibrary.types.cronjob;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

public interface QuartzCronJobJobFactory extends JobFactory
{
	void setJobFactory(final Class<? extends Job> clazz, final Supplier<Job> supplier);

	static QuartzCronJobJobFactory New()
	{
		return new Default();
	}

	final class Default implements QuartzCronJobJobFactory
	{
		private final Map<Class<? extends Job>, Supplier<Job>> jobSupliers = new HashMap<>();

		private Default()
		{
		}

		@Override
		public void setJobFactory(final Class<? extends Job> clazz, final Supplier<Job> supplier)
		{
			this.jobSupliers.put(clazz, supplier);
		}

		@Override
		public Job newJob(final TriggerFiredBundle bundle, final Scheduler scheduler) throws SchedulerException
		{
			final var jobClass = bundle.getJobDetail().getJobClass();
			final var supplier = this.jobSupliers.get(jobClass);

			if (supplier == null)
			{
				throw new SchedulerException("No factory set for job class '" + jobClass.getName() + "'");
			}

			try
			{
				return supplier.get();
			}
			catch (final Exception e)
			{
				throw new SchedulerException("Failed to instantiate job class '" + jobClass.getName() + "'", e);
			}
		}
	}
}
