package org.apache.accumulo.examples.helloworld;


import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;


public class ReadData {
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		if (((args.length) < 5) || ((args.length) > 7)) {
			System.out.println("Usage: HADOOP_CLASSPATH=accumulo/conf:thriftjar:zookeeperjar:accumulo-corejar hadoop jar accumulo-examplesjar accumulo.examples.helloworld.ReadData <instance name> <zoo keepers> <tablename> <username> <password> [startkey [endkey]]");
			System.exit(1);
		}
		String instanceName = args[0];
		String zooKeepers = args[1];
		String tableName = args[2];
		String user = args[3];
		byte[] pass = args[4].getBytes();
		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
		Connector connector = instance.getConnector(user, pass);
		Scanner scan = connector.createScanner(tableName, Constants.NO_AUTHS);
		Key start = ((args.length) < 5) ? null : new Key(new Text(args[5]));
		Key end = ((args.length) < 6) ? null : new Key(new Text(args[6]));
		scan.setRange(new Range(start, end));
		Iterator<Map.Entry<Key, Value>> iter = scan.iterator();
		while (iter.hasNext()) {
			Map.Entry<Key, Value> e = iter.next();
			Text colf = e.getKey().getColumnFamily();
			Text colq = e.getKey().getColumnQualifier();
			System.out.print(((((("row: " + (e.getKey().getRow())) + ", colf: ") + colf) + ", colq: ") + colq));
			System.out.println((", value: " + (e.getValue().toString())));
		} 
	}
}

