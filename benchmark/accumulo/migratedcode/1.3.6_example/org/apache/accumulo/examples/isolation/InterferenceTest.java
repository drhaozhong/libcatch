package org.apache.accumulo.examples.isolation;


import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;


public class InterferenceTest {
	private static final int NUM_ROWS = 5000;

	private static final int NUM_COLUMNS = 113;

	static class Writer implements Runnable {
		private BatchWriter bw;

		Writer(BatchWriter bw) {
			this.bw = bw;
		}

		@Override
		public void run() {
			int row = 0;
			int value = 0;
			while (true) {
				Mutation m = new Mutation(new Text(String.format("%03d", row)));
				row = (row + 1) % (InterferenceTest.NUM_ROWS);
				for (int cq = 0; cq < (InterferenceTest.NUM_COLUMNS); cq++)
					m.put(new Text("000"), new Text(String.format("%04d", cq)), new Value(("" + value).getBytes()));

				value++;
				try {
					bw.addMutation(m);
				} catch (MutationsRejectedException e) {
					e.printStackTrace();
					System.exit((-1));
				}
			} 
		}
	}

	static class Reader implements Runnable {
		private Scanner scanner;

		Reader(Scanner scanner) {
			this.scanner = scanner;
		}

		@Override
		public void run() {
			while (true) {
				ByteSequence row = null;
				int count = 0;
				HashSet<String> values = new HashSet<String>();
				for (Map.Entry<Key, Value> entry : scanner) {
					if (row == null)
						row = entry.getKey().getRowData();

					if (!(row.equals(entry.getKey().getRowData()))) {
						if (count != (InterferenceTest.NUM_COLUMNS))
							System.err.println(((("ERROR Did not see " + (InterferenceTest.NUM_COLUMNS)) + " columns in row ") + row));

						if ((values.size()) > 1)
							System.err.println(((("ERROR Columns in row " + row) + " had multiple values ") + values));

						row = entry.getKey().getRowData();
						count = 0;
						values.clear();
					}
					count++;
					values.add(entry.getValue().toString());
				}
				if ((count > 0) && (count != (InterferenceTest.NUM_COLUMNS)))
					System.err.println(((("ERROR Did not see " + (InterferenceTest.NUM_COLUMNS)) + " columns in row ") + row));

				if ((values.size()) > 1)
					System.err.println(((("ERROR Columns in row " + row) + " had multiple values ") + values));

			} 
		}
	}

	public static void main(String[] args) throws Exception {
		if ((args.length) != 6) {
			System.out.println((("Usage : " + (InterferenceTest.class.getName())) + " <instance name> <zookeepers> <user> <password> <table> true|false"));
			System.out.println("          The last argument determines if scans should be isolated.  When false, expect to see errors");
			return;
		}
		ZooKeeperInstance zki = new ZooKeeperInstance(args[0], args[1]);
		Connector conn = zki.getConnector(args[2], args[3].getBytes());
		String table = args[4];
		new Thread(new InterferenceTest.Writer(conn.createBatchWriter(table, 10000000, 60000L, 3))).start();
		if (Boolean.parseBoolean(args[5]))
			new Thread(new InterferenceTest.Reader(new IsolatedScanner(conn.createScanner(table, Constants.NO_AUTHS)))).start();
		else
			new Thread(new InterferenceTest.Reader(conn.createScanner(table, Constants.NO_AUTHS))).start();

	}
}

