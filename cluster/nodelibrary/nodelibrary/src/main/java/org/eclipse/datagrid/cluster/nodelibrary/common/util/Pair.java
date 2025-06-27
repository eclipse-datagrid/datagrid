package org.eclipse.datagrid.cluster.nodelibrary.common.util;

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

public class Pair<L, R>
{
	public static <L, R> Pair<L, R> of(final L left, final R right)
	{
		return new Pair<>(left, right);
	}

	private final L left;
	private final R right;

	public Pair(final L left, final R right)
	{
		this.left = left;
		this.right = right;
	}

	public L left()
	{
		return this.left;
	}

	public R right()
	{
		return this.right;
	}
}
