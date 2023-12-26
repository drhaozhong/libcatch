package org.apache.accumulo.examples.shard;


import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;


public class Query {
	public static void main(String[] args) throws Exception {
		if ((args.length) < 6) {
			System.err.println((("Usage : " + (Query.class.getName())) + " <instance> <zoo keepers> <table> <user> <pass> <term>{ <term>}"));
			System.exit((-1));
		}
		String instance = args[0];
		String zooKeepers = args[1];
		String table = args[2];
		String user = args[3];
		String pass = args[4];
		ZooKeeperInstance zki = new ZooKeeperInstance(instance, zooKeepers);
		Connector conn = zki.getConnector(user, pass.getBytes());
		BatchScanner bs = conn.createBatchScanner(table, Constants.NO_AUTHS, 20);
		Text[] columns = new Text[(args.length) - 5];
		for (int i = 5; i < (args.length); i++) {
			columns[(i - 5)] = new Text(args[i]);
		}
		bs.setScanIterators(20, IntersectingIterator.class.getName(), "ii");
		bs.setRanges(Collections.singleton(new Range()));
		for (Map.Entry<Key, Value> entry : bs) {
			System.out.println(("  " + (entry.getKey().getColumnQualifier())));
		}
	}
}

