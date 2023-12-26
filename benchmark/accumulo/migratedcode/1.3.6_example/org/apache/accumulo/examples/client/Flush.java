package org.apache.accumulo.examples.client;


import java.io.PrintStream;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;


public class Flush {
	public static void main(String[] args) {
		if ((args.length) != 5) {
			System.err.println("Usage: accumulo accumulo.examples.client.Flush <instance name> <zoo keepers> <username> <password> <tableName>");
			return;
		}
		String instanceName = args[0];
		String zooKeepers = args[1];
		String user = args[2];
		String password = args[3];
		String table = args[4];
		Connector connector;
		try {
			ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
			connector = instance.getConnector(user, password.getBytes());
			connector.tableOperations().flush(table);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}

