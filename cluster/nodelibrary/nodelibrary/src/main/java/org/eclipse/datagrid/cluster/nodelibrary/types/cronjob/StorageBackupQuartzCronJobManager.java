package org.eclipse.datagrid.cluster.nodelibrary.types.cronjob;

import org.eclipse.datagrid.cluster.nodelibrary.types.StorageBackupManager;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface StorageBackupQuartzCronJobManager extends QuartzCronJobManager
{
	static StorageBackupQuartzCronJobManager New(final StorageBackupManager backupManager)
	{
		return new Default(notNull(backupManager));
	}

	final class Default implements StorageBackupQuartzCronJobManager
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageBackupQuartzCronJobManager.class);
		private final StorageBackupManager backupManager;

		private Default(final StorageBackupManager backupManager)
		{
			this.backupManager = backupManager;
		}

		@Override
		public Job create()
		{
			LOG.debug("Instancing new storage backup quartz cron job");
			return new StorageBackupQuartzCronJob(this.backupManager);
		}
	}

	@DisallowConcurrentExecution
	final class StorageBackupQuartzCronJob implements Job
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageBackupQuartzCronJob.class);
		private final StorageBackupManager backupManager;

		private StorageBackupQuartzCronJob(final StorageBackupManager backupManager)
		{
			this.backupManager = backupManager;
		}

		@Override
		public void execute(final JobExecutionContext context) throws JobExecutionException
		{
			LOG.info("Issuing full backup");
			this.backupManager.createStorageBackup(false);
		}
	}
}
