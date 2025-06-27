package org.eclipse.datagrid.cluster.nodelibrary.common.impl._default;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.function.Supplier;

import org.eclipse.serializer.persistence.types.PersistenceCommitListener;
import org.eclipse.serializer.persistence.types.PersistenceObjectRegistrationListener;
import org.eclipse.serializer.persistence.types.Storer;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.embedded.configuration.types.EmbeddedStorageConfiguration;
import org.eclipse.store.storage.embedded.configuration.types.EmbeddedStorageConfigurationBuilder;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageManager;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterEnv;
import org.eclipse.datagrid.cluster.nodelibrary.common.StorageLimitChecker;
import org.eclipse.datagrid.cluster.nodelibrary.common.exception.NotADistributorException;
import org.eclipse.datagrid.cluster.nodelibrary.common.storage.ActivatableStorageBinaryDataDistributor;
import org.eclipse.datagrid.cluster.nodelibrary.common.storage.ActivatableStorageBinaryDataMerger;
import org.eclipse.datagrid.cluster.nodelibrary.common.storage.ClusterStorageBinaryDataDistributorKafka;
import org.eclipse.datagrid.cluster.nodelibrary.common.storage.MyStorageBinaryDataClientKafka;
import org.eclipse.datagrid.storage.distributed.types.DistributedStorage;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMerger;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAcceptor;

public class NodeDefaultClusterStorageManager<T> extends DefaultClusterStorageManager.Abstract<T>
{
	private static final Logger LOG = LoggerFactory.getLogger(NodeDefaultClusterStorageManager.class);
	
	private final ActivatableStorageBinaryDataDistributor  distributor;
	private final ActivatableStorageBinaryDataMerger       merger;
	private final MyStorageBinaryDataClientKafka           dataClient;
	private final ClusterStorageBinaryDataDistributorKafka kafkaDistributor;
	private       boolean                                  isDistributor;
	private final EmbeddedStorageManager                   storage;

	public NodeDefaultClusterStorageManager(final Supplier<T> rootSupplier, final boolean async)
	{
		this(rootSupplier, EmbeddedStorageConfiguration.Builder(), async);
	}

	public NodeDefaultClusterStorageManager(
		final Supplier<T> rootSupplier,
		final EmbeddedStorageConfigurationBuilder config,
		final boolean async
	)
	{
		LOG.info("Initializing storage.");

		try
		{
			StorageLimitChecker.get().start();
		}
		catch (final Exception e)
		{
			throw new RuntimeException(e);
		}
		Runtime.getRuntime().addShutdownHook(new Thread(() ->
		{
			try
			{
				StorageLimitChecker.get().stop();
			}
			catch (final SchedulerException e)
			{
				LOG.error("Failed to shutdown storage limit checker.", e);
			}
		}));
		final var storagePath = Paths.get("/storage/storage");
		final long storageOffset;

		try
		{
			final var offsetFile = Paths.get("/storage/offset");
			if (Files.notExists(offsetFile))
			{
				Files.writeString(
					offsetFile,
					Long.toString(Long.MIN_VALUE),
					StandardCharsets.UTF_8,
					StandardOpenOption.CREATE
				);
			}
			storageOffset = Long.parseLong(Files.readString(offsetFile).trim());
		}
		catch (final IOException e)
		{
			throw new RuntimeException("Failed to load storage offset from file", e);
		}

		final String topic = ClusterEnv.kafkaTopicName();

		LOG.info("Using offset {}", storageOffset);

		this.kafkaDistributor = async ? ClusterStorageBinaryDataDistributorKafka.Async(topic)
			: ClusterStorageBinaryDataDistributorKafka.Sync(topic);
		this.distributor = new ActivatableStorageBinaryDataDistributor(
			StorageBinaryDataDistributor.Caching(this.kafkaDistributor)
		);

		final var foundation = DistributedStorage.configureWriting(
			config.setStorageDirectory(storagePath.toString()).createEmbeddedStorageFoundation(),
			this.distributor
		);

		this.storage = foundation.start();

		if (this.storage.root() == null)
		{
			final var root = rootSupplier.get();
			if (root instanceof Lazy)
			{
				this.storage.setRoot(root);
			}
			else
			{
				this.storage.setRoot(Lazy.Reference(root));
			}
			this.storage.storeRoot();
		}

		this.merger = new ActivatableStorageBinaryDataMerger(
			StorageBinaryDataMerger.New(
				foundation.getConnectionFoundation(),
				this.storage,
				ObjectGraphUpdateHandler.Synchronized()
			)
		);

		this.dataClient = new MyStorageBinaryDataClientKafka(
			topic,
			topic + "-" + ClusterEnv.myPodName() + "-" + this.getRandomAlphaNumeric(8),
			storageOffset,
			StorageBinaryDataPacketAcceptor.New(this.merger)
		);

		this.dataClient.start();
	}

	private String getRandomAlphaNumeric(final int length)
	{
		// Generates values of 48 '0' to 122 'z'
		return new SecureRandom().ints(48, 123)
			// Filter out special characters
			.filter(i -> (i < 58 || i > 64) && (i < 91 || i > 96))
			.limit(length)
			.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
			.toString();
	}

	private void ensureDistribution()
	{
		if (!this.isDistributor())
		{
			throw new NotADistributorException("This node is currently not a distributor");
		}
	}

	@Override
	protected StorageManager delegate()
	{
		return this.storage;
	}

	@Override
	public boolean shutdown()
	{
		LOG.info("Disposing Cluster Resources");
		this.dataClient.dispose();
		this.distributor.dispose();
		this.storage.close();
		return super.shutdown();
	}

	@Override
	public long getCurrentOffset()
	{
		return Math.max(this.dataClient.getStorageOffset(), this.kafkaDistributor.getStorageOffset());
	}

	@SuppressWarnings("unchecked")
	@Override
	public Lazy<T> root()
	{
		return (Lazy<T>)this.storage.root();
	}

	@Override
	public void activateDistribution()
	{
		if (this.isDistributor())
		{
			throw new RuntimeException("Distribution is already enabled.");
		}

		this.dataClient.unready();

		// Wait for initial messages to be consumed
		while (!this.dataClient.isReady() && this.dataClient.isActive())
		{
			try
			{
				Thread.sleep(1000);
			}
			catch (final InterruptedException e)
			{
				throw new RuntimeException(e);
			}
		}

		LOG.info("Turning on distribution.");
		this.merger.setActive(false);
		this.kafkaDistributor.setStorageOffset(this.dataClient.getStorageOffset());
		this.distributor.setActive(true);
		this.isDistributor = true;
	}

	@Override
	public boolean isDistributor()
	{
		return this.isDistributor;
	}

	@Override
	public long store(final Object instance)
	{
		this.ensureDistribution();
		return super.store(instance);
	}

	@Override
	public long[] storeAll(final Object... instances)
	{
		this.ensureDistribution();
		return super.storeAll(instances);
	}

	@Override
	public void storeAll(final Iterable<?> instances)
	{
		this.ensureDistribution();
		super.storeAll(instances);
	}

	@Override
	public long storeRoot()
	{
		this.ensureDistribution();
		return super.storeRoot();
	}

	@Override
	public Storer createEagerStorer()
	{
		return new NodeDefaultClusterStorerAdapter(super.createEagerStorer());
	}

	@Override
	public Storer createLazyStorer()
	{
		return new NodeDefaultClusterStorerAdapter(super.createLazyStorer());
	}

	@Override
	public Storer createStorer()
	{
		return new NodeDefaultClusterStorerAdapter(super.createStorer());
	}

	private class NodeDefaultClusterStorerAdapter implements Storer
	{
		private final Storer storer;

		private NodeDefaultClusterStorerAdapter(final Storer storer)
		{
			this.storer = storer;
		}

		@Override
		public long store(final Object instance)
		{
			return this.storer.store(instance);
		}

		@Override
		public long[] storeAll(final Object... instances)
		{
			return this.storer.storeAll(instances);
		}

		@Override
		public void storeAll(final Iterable<?> instances)
		{
			this.storer.storeAll(instances);
		}

		@Override
		public Object commit()
		{
			NodeDefaultClusterStorageManager.this.ensureDistribution();
			return this.storer.commit();
		}

		@Override
		public void clear()
		{
			this.storer.clear();
		}

		@Override
		public boolean skipMapped(final Object instance, final long objectId)
		{
			return this.storer.skipMapped(instance, objectId);
		}

		@Override
		public boolean skip(final Object instance)
		{
			return this.storer.skip(instance);
		}

		@Override
		public boolean skipNulled(final Object instance)
		{
			return this.storer.skipNulled(instance);
		}

		@Override
		public long size()
		{
			return this.storer.size();
		}

		@Override
		public long currentCapacity()
		{
			return this.storer.currentCapacity();
		}

		@Override
		public long maximumCapacity()
		{
			return this.storer.maximumCapacity();
		}

		@Override
		public Storer reinitialize()
		{
			return this.storer.reinitialize();
		}

		@Override
		public Storer reinitialize(final long initialCapacity)
		{
			return this.storer.reinitialize(initialCapacity);
		}

		@Override
		public Storer ensureCapacity(final long desiredCapacity)
		{
			return this.storer.ensureCapacity(desiredCapacity);
		}

		@Override
		public void registerCommitListener(final PersistenceCommitListener listener)
		{
			this.storer.registerCommitListener(listener);
		}
		
		@Override
		public void registerRegistrationListener(final PersistenceObjectRegistrationListener listener)
		{
			this.storer.registerRegistrationListener(listener);
		}
		
		@Override
		public boolean isEmpty()
		{
			return this.storer.isEmpty();
		}
	}
}
