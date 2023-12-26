package org.apache.accumulo.examples.simple.client;


import com.beust.jcommander.Parameter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.cli.BatchScannerOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class RandomBatchScanner {
	private static final Logger log = Logger.getLogger(CountingVerifyingReceiver.class);

	static void generateRandomQueries(int num, long min, long max, Random r, HashSet<Range> ranges, HashMap<Text, Boolean> expectedRows) {
		RandomBatchScanner.log.info(String.format("Generating %,d random queries...", num));
		while ((ranges.size()) < num) {
			long rowid = ((Math.abs(r.nextLong())) % (max - min)) + min;
			Text row1 = new Text(String.format("row_%010d", rowid));
			Range range = new Range(new Text(row1));
			ranges.add(range);
			expectedRows.put(row1, false);
		} 
		RandomBatchScanner.log.info("finished");
	}

	private static void printRowsNotFound(HashMap<Text, Boolean> expectedRows) {
		int count = 0;
		for (Map.Entry<Text, Boolean> entry : expectedRows.entrySet())
			if (!(entry.getValue()))
				count++;


		if (count > 0)
			RandomBatchScanner.log.warn((("Did not find " + count) + " rows"));

	}

	static void doRandomQueries(int num, long min, long max, int evs, Random r, BatchScanner tsbr) {
		HashSet<Range> ranges = new HashSet<Range>(num);
		HashMap<Text, Boolean> expectedRows = new HashMap<Text, Boolean>();
		RandomBatchScanner.generateRandomQueries(num, min, max, r, ranges, expectedRows);
		tsbr.setRanges(ranges);
		CountingVerifyingReceiver receiver = new CountingVerifyingReceiver(expectedRows, evs);
		long t1 = System.currentTimeMillis();
		for (Map.Entry<Key, Value> entry : tsbr) {
			receiver.receive(entry.getKey(), entry.getValue());
		}
		long t2 = System.currentTimeMillis();
		RandomBatchScanner.log.info(String.format("%6.2f lookups/sec %6.2f secs%n", (num / ((t2 - t1) / 1000.0)), ((t2 - t1) / 1000.0)));
		RandomBatchScanner.log.info(String.format("num results : %,d%n", receiver.count));
		RandomBatchScanner.printRowsNotFound(expectedRows);
	}

	public static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--min", description = "miniumum row that will be generated")
		long min = 0;

		@Parameter(names = "--max", description = "maximum ow that will be generated")
		long max = 0;

		@Parameter(names = "--num", required = true, description = "number of ranges to generate")
		int num = 0;

		@Parameter(names = "--size", required = true, description = "size of the value to write")
		int size = 0;

		@Parameter(names = "--seed", description = "seed for pseudo-random number generator")
		Long seed = null;
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		RandomBatchScanner.Opts opts = new RandomBatchScanner.Opts();
		BatchScannerOpts bsOpts = new BatchScannerOpts();
		opts.parseArgs(RandomBatchScanner.class.getName(), args, bsOpts);
		Connector connector = opts.getConnector();
		BatchScanner batchReader = connector.createBatchScanner(opts.tableName, opts.auths, bsOpts.scanThreads);
		batchReader.setTimeout(bsOpts.scanTimeout, TimeUnit.MILLISECONDS);
		Random r;
		if ((opts.seed) == null)
			r = new Random();
		else
			r = new Random(opts.seed);

		RandomBatchScanner.doRandomQueries(opts.num, opts.min, opts.max, opts.size, r, batchReader);
		System.gc();
		System.gc();
		System.gc();
		if ((opts.seed) == null)
			r = new Random();
		else
			r = new Random(opts.seed);

		RandomBatchScanner.doRandomQueries(opts.num, opts.min, opts.max, opts.size, r, batchReader);
		batchReader.close();
	}
}

