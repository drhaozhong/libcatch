package org.apache.accumulo.examples.simple.isolation;


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
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class InterferenceTest {
	private static final int NUM_ROWS = 500;

	private static final int NUM_COLUMNS = 113;

	private static long iterations;

	private static final Logger log = Logger.getLogger(InterferenceTest.class);

	static class Writer implements Runnable {
		private BatchWriter bw;

		Writer(BatchWriter bw) {
			this.bw = bw;
		}

		@Override
		public void run() {
			int row = 0;
			int value = 0;
			for (long i = 0; i < (InterferenceTest.iterations); i++) {
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
			try {
				bw.close();
			} catch (MutationsRejectedException e) {
				InterferenceTest.log.error(e, e);
			}
		}
	}

	static class Reader implements Runnable {
		private Scanner scanner;

		volatile boolean stop = false;

		Reader(Scanner scanner) {
			this.scanner = scanner;
		}

		@Override
		public void run() {
			while (!(stop)) {
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

		public void stopNow() {
			stop = true;
		}
	}

	public static void main(String[] args) throws Exception {
		if ((args.length) != 7) {
			System.out.println((("Usage : " + (InterferenceTest.class.getName())) + " <instance name> <zookeepers> <user> <password> <table> <iterations> true|false"));
			System.out.println("          The last argument determines if scans should be isolated.  When false, expect to see errors");
			return;
		}
		ZooKeeperInstance zki = new ZooKeeperInstance(args[0], args[1]);
		Connector conn = zki.getConnector(args[2], args[3].getBytes());
		String table = args[4];
		InterferenceTest.iterations = Long.parseLong(args[5]);
		if ((InterferenceTest.iterations) < 1)
			InterferenceTest.iterations = Long.MAX_VALUE;

		if (!(conn.tableOperations().exists(table)))
			conn.tableOperations().create(table);

		Thread writer = new Thread(new InterferenceTest.Writer(conn.createBatchWriter(table, 10000000, 60000L, 3)));
		writer.start();
		InterferenceTest.Reader r;
		if (Boolean.parseBoolean(args[6]))
			r = new InterferenceTest.Reader(new IsolatedScanner(conn.createScanner(table, Constants.NO_AUTHS)));
		else
			r = new InterferenceTest.Reader(conn.createScanner(table, Constants.NO_AUTHS));

		Thread reader;
		reader = new Thread(r);
		reader.start();
		writer.join();
		r.stopNow();
		reader.join();
		System.out.println("finished");
	}
}

