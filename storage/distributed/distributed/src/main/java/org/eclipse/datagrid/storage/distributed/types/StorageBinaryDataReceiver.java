package org.eclipse.datagrid.storage.distributed.types;

import org.eclipse.serializer.persistence.binary.types.Binary;

public interface StorageBinaryDataReceiver
{
	public void receiveData(Binary data);

	public void receiveTypeDictionary(String typeDictionaryData);
}
