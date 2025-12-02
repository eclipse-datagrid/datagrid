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


public final class StorageNodeRestRouteConfigurations
{
	public static final class MediaTypes
	{
		private static final String WILDCARD = "*/*";
		private static final String APPLICATION_JSON = "application/json";
		private static final String TEXT_PLAIN = "text/plain";

		private MediaTypes()
		{
		}
	}

	public static final String ROOT_PATH = "/eclipse-datagrid";

	public static final class GetDistributor
	{
		public static final String PATH = "/distributor";
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private GetDistributor()
		{
		}
	}

	public static final class PostActivateDistributorStart
	{
		public static final String PATH = "/activate-distributor/start";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private PostActivateDistributorStart()
		{
		}
	}

	public static final class PostActivateDistributorFinish
	{
		public static final String PATH = "/activate-distributor/finish";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private PostActivateDistributorFinish()
		{
		}
	}

	public static final class GetHealth
	{
		public static final String PATH = "/health";
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private GetHealth()
		{
		}
	}

	public static final class GetHealthReady
	{
		public static final String PATH = "/health/ready";
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private GetHealthReady()
		{
		}
	}

	public static final class GetStorageBytes
	{
		public static final String PATH = "/storage-bytes";
		public static final String PRODUCES = MediaTypes.TEXT_PLAIN;

		private GetStorageBytes()
		{
		}
	}

	public static final class PostBackup
	{
		public static final String PATH = "/backup";
		public static final String CONSUMES = MediaTypes.APPLICATION_JSON;
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		public static final class Body
		{
			private Boolean useManualSlot;

			public Boolean getUseManualSlot()
			{
				return this.useManualSlot;
			}

			public void setUseManualSlot(final Boolean useManualSlot)
			{
				this.useManualSlot = useManualSlot;
			}
		}

		private PostBackup()
		{
		}
	}

	public static final class GetBackup
	{
		public static final String PATH = "/backup";
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private GetBackup()
		{
		}
	}

	public static final class PostUpdates
	{
		public static final String PATH = "/updates";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private PostUpdates()
		{
		}
	}

	public static final class GetUpdates
	{
		public static final String PATH = "/updates";
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private GetUpdates()
		{
		}
	}

	public static final class PostResumeUpdates
	{
		public static final String PATH = "/resume-updates";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private PostResumeUpdates()
		{
		}
	}

	public static final class PostGc
	{
		public static final String PATH = "/gc";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private PostGc()
		{
		}
	}

	public static final class GetGc
	{
		public static final String PATH = "/gc";
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private GetGc()
		{
		}
	}

	private StorageNodeRestRouteConfigurations()
	{
	}
}
