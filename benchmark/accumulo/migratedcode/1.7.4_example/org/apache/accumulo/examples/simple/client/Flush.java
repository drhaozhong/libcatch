package org.apache.accumulo.examples.simple.client;


import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;


public class Flush {
	public static void main(String[] args) {
		ClientOnRequiredTable opts = new ClientOnRequiredTable();
		opts.parseArgs(Flush.class.getName(), args);
		try {
			Connector connector = opts.getConnector();
			connector.tableOperations().flush(opts.getTableName(), null, null, true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}

