package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

/**
 * A visitor that copies a directory (or file) into another directory. This is intended to be used with
 * {@link Files#walkFileTree(Path, FileVisitor)}.
 */
public class CopyVisitor extends SimpleFileVisitor<Path>
{
    private final Path sourceDirectory;
    private final Path targetDirectory;

    /**
     * Creates a new {@link CopyVisitor} with the given source and target directories.
     *
     * @param sourceDirectory the source directory or file to copy
     * @param targetDirectory the target directory to copy into
     */
    public CopyVisitor(final Path sourceDirectory, final Path targetDirectory)
    {
        this.sourceDirectory = Objects.requireNonNull(sourceDirectory);
        this.targetDirectory = Objects.requireNonNull(targetDirectory);
    }

    private Path resolveForTarget(final Path p)
    {
        return this.targetDirectory.resolve(sourceDirectory.relativize(p));
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException
    {
        final Path newDir = this.resolveForTarget(dir);
        Files.createDirectories(newDir);
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
        throws IOException
    {
        final Path newFile = this.resolveForTarget(file);
        Files.copy(file, newFile);
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc)
        throws IOException
    {
        Objects.requireNonNull(dir);
        if (exc != null)
            throw exc;
        return FileVisitResult.CONTINUE;
    }
}
