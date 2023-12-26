package org.apache.accumulo.examples.simple.shard;


import java.io.PrintStream;
import java.util.Map;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;


public class Reverse {
	public static void main(String[] args) throws Exception {
		if ((args.length) != 6) {
			System.err.println((("Usage : " + (Reverse.class.getName())) + " <instance> <zoo keepers> <shard table> <doc2word table> <user> <pass>"));
			System.exit((-1));
		}
		String instance = args[0];
		String zooKeepers = args[1];
		String inTable = args[2];
		String outTable = args[3];
		String user = args[4];
		String pass = args[5];
		ZooKeeperInstance zki = new ZooKeeperInstance(instance, zooKeepers);
		Connector conn = zki.getConnector(user, pass.getBytes());
		Scanner scanner = conn.createScanner(inTable, Constants.NO_AUTHS);
		BatchWriter bw = conn.createBatchWriter(outTable, 50000000, 600000L, 4);
		for (Map.Entry<Key, Value> entry : scanner) {
			Key key = entry.getKey();
			Mutation m = new Mutation(key.getColumnQualifier());
			m.put(key.getColumnFamily(), new Text(), new Value(new byte[0]));
			bw.addMutation(m);
		}
		bw.close();
	}
}

