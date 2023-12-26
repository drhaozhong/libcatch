package org.apache.accumulo.examples.helloworld;


import java.io.PrintStream;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;


public class InsertWithBatchWriter {
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableExistsException, TableNotFoundException {
		if ((args.length) != 5) {
			System.out.println("Usage: HADOOP_CLASSPATH=thriftjar:zookeeperjar:accumulo-corejar hadoop jar accumulo-examplesjar accumulo.examples.helloworld.InsertWithBatchWriter <instance name> <zoo keepers> <tableName> <username> <password>");
			System.exit(1);
		}
		String instanceName = args[0];
		String zooKeepers = args[1];
		String tableName = args[2];
		String user = args[3];
		byte[] pass = args[4].getBytes();
		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
		Connector connector = instance.getConnector(user, pass);
		MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000L, 300, 4);
		BatchWriter bw = null;
		if (!(connector.tableOperations().exists(tableName)))
			connector.tableOperations().create(tableName);

		bw = mtbw.getBatchWriter(tableName);
		Text colf = new Text("colfam");
		System.out.println("writing ...");
		for (int i = 0; i < 10000; i++) {
			Mutation m = new Mutation(new Text(String.format("row_%d", i)));
			for (int j = 0; j < 5; j++) {
				m.put(colf, new Text(String.format("colqual_%d", j)), new Value(String.format("value_%d_%d", i, j).getBytes()));
			}
			bw.addMutation(m);
			if ((i % 100) == 0)
				System.out.println(i);

		}
		mtbw.close();
	}
}

