package org.eclipse.datagrid.cluster.nodelibrary.types;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;

public interface AfterDataMessageConsumedListener extends AutoCloseable
{
	void onChange(MessageInfo messageInfo) throws NodelibraryException;

	@Override
	void close();
}
