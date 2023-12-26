package org.apache.accumulo.examples.shard;


import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text[];


public class ContinuousQuery {
	public static void main(String[] args) throws Exception {
		if ((args.length) != 7) {
			System.err.println((("Usage : " + (ContinuousQuery.class.getName())) + " <instance> <zoo keepers> <shard table> <doc2word table> <user> <pass> <num query terms>"));
			System.exit((-1));
		}
		String instance = args[0];
		String zooKeepers = args[1];
		String table = args[2];
		String docTable = args[3];
		String user = args[4];
		String pass = args[5];
		int numTerms = Integer.parseInt(args[6]);
		ZooKeeperInstance zki = new ZooKeeperInstance(instance, zooKeepers);
		Connector conn = zki.getConnector(user, pass.getBytes());
		ArrayList<Text[]> randTerms = ContinuousQuery.findRandomTerms(conn.createScanner(docTable, Constants.NO_AUTHS), numTerms);
		Random rand = new Random();
		BatchScanner bs = conn.createBatchScanner(table, Constants.NO_AUTHS, 20);
		bs.setScanIterators(20, IntersectingIterator.class.getName(), "ii");
		while (true) {
			Text[] columns = randTerms.get(rand.nextInt(randTerms.size()));
			bs.setRanges(Collections.singleton(new Range()));
			long t1 = System.currentTimeMillis();
			int count = 0;
			for (@SuppressWarnings("unused")
			Map.Entry<Key, Value> entry : bs) {
				count++;
			}
			long t2 = System.currentTimeMillis();
			System.out.printf("  %s %,d %6.3f\n", Arrays.asList(columns), count, ((t2 - t1) / 1000.0));
		} 
	}

	private static ArrayList<Text[]> findRandomTerms(Scanner scanner, int numTerms) {
		Text currentRow = null;
		ArrayList<Text> words = new ArrayList<Text>();
		ArrayList<Text[]> ret = new ArrayList<Text[]>();
		Random rand = new Random();
		for (Map.Entry<Key, Value> entry : scanner) {
			Key key = entry.getKey();
			if (currentRow == null)
				currentRow = key.getRow();

			if (!(currentRow.equals(key.getRow()))) {
				ContinuousQuery.selectRandomWords(words, ret, rand, numTerms);
				words.clear();
				currentRow = key.getRow();
			}
			words.add(key.getColumnFamily());
		}
		ContinuousQuery.selectRandomWords(words, ret, rand, numTerms);
		return ret;
	}

	private static void selectRandomWords(ArrayList<Text> words, ArrayList<Text[]> ret, Random rand, int numTerms) {
		if ((words.size()) >= numTerms) {
			Collections.shuffle(words, rand);
			Text[] docWords = new Text[numTerms];
			for (int i = 0; i < (docWords.length); i++) {
				docWords[i] = words.get(i);
			}
			ret.add(docWords);
		}
	}
}

