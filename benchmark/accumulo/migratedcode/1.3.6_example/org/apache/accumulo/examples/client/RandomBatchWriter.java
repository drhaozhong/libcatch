package org.apache.accumulo.examples.client;


import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;


public class RandomBatchWriter {
	public static byte[] createValue(long rowid, int dataSize) {
		Random r = new Random(rowid);
		byte[] value = new byte[dataSize];
		r.nextBytes(value);
		for (int j = 0; j < (value.length); j++) {
			value[j] = ((byte) (((255 & (value[j])) % 92) + ' '));
		}
		return value;
	}

	public static Mutation createMutation(long rowid, int dataSize, ColumnVisibility visibility) {
		Text row = new Text(String.format("row_%010d", rowid));
		Mutation m = new Mutation(row);
		byte[] value = RandomBatchWriter.createValue(rowid, dataSize);
		m.put(new Text("foo"), new Text("1"), visibility, new Value(value));
		return m;
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		String seed = null;
		int index = 0;
		String[] processedArgs = new String[13];
		for (int i = 0; i < (args.length); i++) {
			if (args[i].equals("-s")) {
				seed = args[(++i)];
			}else {
				processedArgs[(index++)] = args[i];
			}
		}
		if (index != 13) {
			System.out.println("Usage : RandomBatchWriter [-s <seed>] <instance name> <zoo keepers> <username> <password> <table> <num> <min> <max> <value size> <max memory> <max latency> <num threads> <visibility>");
			return;
		}
		String instanceName = processedArgs[0];
		String zooKeepers = processedArgs[1];
		String user = processedArgs[2];
		byte[] pass = processedArgs[3].getBytes();
		String table = processedArgs[4];
		int num = Integer.parseInt(processedArgs[5]);
		long min = Long.parseLong(processedArgs[6]);
		long max = Long.parseLong(processedArgs[7]);
		int valueSize = Integer.parseInt(processedArgs[8]);
		long maxMemory = Long.parseLong(processedArgs[9]);
		long maxLatency = ((Long.parseLong(processedArgs[10])) == 0) ? Long.MAX_VALUE : Long.parseLong(processedArgs[10]);
		int numThreads = Integer.parseInt(processedArgs[11]);
		String visiblity = processedArgs[12];
		Random r;
		if (seed == null)
			r = new Random();
		else {
			r = new Random(Long.parseLong(seed));
		}
		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
		Connector connector = instance.getConnector(user, pass);
		BatchWriter bw = connector.createBatchWriter(table, maxMemory, maxLatency, numThreads);
		ColumnVisibility cv = new ColumnVisibility(visiblity);
		for (int i = 0; i < num; i++) {
			long rowid = ((Math.abs(r.nextLong())) % (max - min)) + min;
			Mutation m = RandomBatchWriter.createMutation(rowid, valueSize, cv);
			bw.addMutation(m);
		}
		try {
			bw.close();
		} catch (MutationsRejectedException e) {
			if ((e.getAuthorizationFailures().size()) > 0) {
				HashSet<String> tables = new HashSet<String>();
				for (KeyExtent ke : e.getAuthorizationFailures()) {
					tables.add(ke.getTableId().toString());
				}
				System.err.println(("ERROR : Not authorized to write to tables : " + tables));
			}
			if ((e.getConstraintViolationSummaries().size()) > 0) {
				System.err.println(("ERROR : Constraint violations occurred : " + (e.getConstraintViolationSummaries())));
			}
		}
	}
}

