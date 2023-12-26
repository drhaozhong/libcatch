package org.apache.accumulo.examples.simple.isolation;


import com.beust.jcommander.Parameter;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
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

	private static final Logger log = Logger.getLogger(InterferenceTest.class);

	static class Writer implements Runnable {
		private final BatchWriter bw;

		private final long iterations;

		Writer(BatchWriter bw, long iterations) {
			this.bw = bw;
			this.iterations = iterations;
		}

		@Override
		public void run() {
			int row = 0;
			int value = 0;
			for (long i = 0; i < (iterations); i++) {
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

	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--iterations", description = "number of times to run", required = true)
		long iterations = 0;

		@Parameter(names = "--isolated", description = "use isolated scans")
		boolean isolated = false;
	}

	public static void main(String[] args) throws Exception {
		InterferenceTest.Opts opts = new InterferenceTest.Opts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(InterferenceTest.class.getName(), args, bwOpts);
		if ((opts.iterations) < 1)
			opts.iterations = Long.MAX_VALUE;

		Connector conn = opts.getConnector();
		if (!(conn.tableOperations().exists(opts.tableName))) {
		}
		Thread writer = new Thread(new InterferenceTest.Writer(conn.createBatchWriter(opts.tableName, bwOpts.getBatchWriterConfig()), opts.iterations));
		writer.start();
		InterferenceTest.Reader r;
		if (opts.isolated)
			r = new InterferenceTest.Reader(new IsolatedScanner(conn.createScanner(opts.tableName, opts.auths)));
		else
			r = new InterferenceTest.Reader(conn.createScanner(opts.tableName, opts.auths));

		Thread reader;
		reader = new Thread(r);
		reader.start();
		writer.join();
		r.stopNow();
		reader.join();
		System.out.println("finished");
	}
}

