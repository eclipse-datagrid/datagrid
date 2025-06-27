package org.eclipse.datagrid.cluster.nodelibrary.springboot;

/*-
 * #%L
 * Eclipse DataGrid Cluster Nodelibrary Spring Boot
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
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.eclipse.datagrid.cluster.nodelibrary.common.ClusterStorageManager;
import org.eclipse.datagrid.cluster.nodelibrary.common.StorageClusterControllerBase;

@RestController
@RequestMapping(StorageClusterControllerBase.CONTROLLER_PATH)
public class StorageClusterController extends StorageClusterControllerBase
{
	private final Logger logger = LoggerFactory.getLogger(StorageClusterController.class);

	public StorageClusterController(final ClusterStorageManager<?> storageManager)
	{
		super(Optional.of(() -> storageManager));
	}

	@GetMapping(value = "/microstream-distributor", produces = MediaType.TEXT_PLAIN_VALUE)
	public String distributionActive()
	{
		return Boolean.toString(this.internalDistributionActive());
	}

	@PostMapping("/microstream-activate-distributor")
	public void activateDistributor()
	{
		this.internalActivateDistributor();
	}

	@GetMapping("/microstream-health")
	public void checkHealth()
	{
	}

	@GetMapping(value = "/microstream-storage-bytes", produces = MediaType.TEXT_PLAIN_VALUE)
	public String storageBytes()
	{
		return this.internalGetUsedUpStorageBytes();
	}

	@GetMapping("/microstream-health/ready")
	public ResponseEntity<Void> readyCheck()
	{
		return new ResponseEntity<>(
			this.isReady() ? org.springframework.http.HttpStatus.OK
				: org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR
		);
	}

	@PostMapping(value = "/microstream-uploadStorage", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
	@Async
	public CompletableFuture<Void> uploadStorage(@NonNull @RequestBody final InputStream storage) throws IOException
	{
		this.internalUploadStorage(storage);
		return CompletableFuture.completedFuture(null);
	}

	@PostMapping(value = "/microstream-backup", produces = MediaType.TEXT_PLAIN_VALUE)
	public ResponseEntity<String> createBackupNow()
	{
		try
		{
			this.internalCreateBackupNow();
			return new ResponseEntity<>(HttpStatus.OK);
		}
		catch (final Exception e)
		{
			this.logger.error(e.getMessage(), e);
			return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@PostMapping(value = "/microstream-updates", consumes = MediaType.TEXT_PLAIN_VALUE)
	public void stopUpdates()
	{
		this.internalStopUpdates();
	}

	@PostMapping("/microstream-gc")
	public void callGc()
	{
		this.internalCallGc();
	}
}
