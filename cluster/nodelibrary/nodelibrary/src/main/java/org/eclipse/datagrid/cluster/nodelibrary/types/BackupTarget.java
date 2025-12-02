package org.eclipse.datagrid.cluster.nodelibrary.types;

public enum BackupTarget
{
	SAAS, ONPREM;

	/**
	 * Tries to parse the string into the appropriate backup target. If it fails
	 * `null` is returned.
	 */
	public static BackupTarget parse(final String s)
	{
		if (s == null)
		{
			return null;
		}

		switch (s)
		{
		case "SAAS":
			return SAAS;
		case "ONPREM":
			return ONPREM;
		default:
			return null;
		}
	}
}
