package org.apache.accumulo.examples.simple.client;


import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
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
		RandomBatchScanner.log.info(String.format("%6.2f lookups/sec %6.2f secs\n", (num / ((t2 - t1) / 1000.0)), ((t2 - t1) / 1000.0)));
		RandomBatchScanner.log.info(String.format("num results : %,d\n", receiver.count));
		RandomBatchScanner.printRowsNotFound(expectedRows);
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		String seed = null;
		int index = 0;
		String[] processedArgs = new String[11];
		for (int i = 0; i < (args.length); i++) {
			if (args[i].equals("-s")) {
				seed = args[(++i)];
			}else {
				processedArgs[(index++)] = args[i];
			}
		}
		if (index != 11) {
			System.out.println("Usage : RandomBatchScanner [-s <seed>] <instance name> <zoo keepers> <username> <password> <table> <num> <min> <max> <expected value size> <num threads> <auths>");
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
		int expectedValueSize = Integer.parseInt(processedArgs[8]);
		int numThreads = Integer.parseInt(processedArgs[9]);
		String auths = processedArgs[10];
		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
		Connector connector = instance.getConnector(user, pass);
		BatchScanner tsbr = connector.createBatchScanner(table, new Authorizations(auths.split(",")), numThreads);
		Random r;
		if (seed == null)
			r = new Random();
		else
			r = new Random(Long.parseLong(seed));

		RandomBatchScanner.doRandomQueries(num, min, max, expectedValueSize, r, tsbr);
		System.gc();
		System.gc();
		System.gc();
		if (seed == null)
			r = new Random();
		else
			r = new Random(Long.parseLong(seed));

		RandomBatchScanner.doRandomQueries(num, min, max, expectedValueSize, r, tsbr);
		tsbr.close();
	}
}

