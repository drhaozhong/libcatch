package org.apache.accumulo.examples.simple.dirlist;


import com.beust.jcommander.Parameter;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;


public class FileCount {
	private int entriesScanned;

	private int inserts;

	private FileCount.Opts opts;

	private ScannerOpts scanOpts;

	private BatchWriterOpts bwOpts;

	private static class CountValue {
		int dirCount = 0;

		int fileCount = 0;

		int recursiveDirCount = 0;

		int recusiveFileCount = 0;

		void set(Value val) {
			String[] sa = val.toString().split(",");
			dirCount = Integer.parseInt(sa[0]);
			fileCount = Integer.parseInt(sa[1]);
			recursiveDirCount = Integer.parseInt(sa[2]);
			recusiveFileCount = Integer.parseInt(sa[3]);
		}

		Value toValue() {
			return new Value((((((((dirCount) + ",") + (fileCount)) + ",") + (recursiveDirCount)) + ",") + (recusiveFileCount)).getBytes());
		}

		void incrementFiles() {
			(fileCount)++;
			(recusiveFileCount)++;
		}

		void incrementDirs() {
			(dirCount)++;
			(recursiveDirCount)++;
		}

		public void clear() {
			dirCount = 0;
			fileCount = 0;
			recursiveDirCount = 0;
			recusiveFileCount = 0;
		}

		public void incrementRecursive(FileCount.CountValue other) {
			recursiveDirCount += other.recursiveDirCount;
			recusiveFileCount += other.recusiveFileCount;
		}
	}

	private int findMaxDepth(Scanner scanner, int min, int max) {
		int mid = min + ((max - min) / 2);
		return findMaxDepth(scanner, min, mid, max);
	}

	private int findMaxDepth(Scanner scanner, int min, int mid, int max) {
		if (max < min)
			return -1;

		scanner.setRange(new Range(String.format("%03d", mid), true, String.format("%03d", (mid + 1)), false));
		if (scanner.iterator().hasNext()) {
			int ret = findMaxDepth(scanner, (mid + 1), max);
			if (ret == (-1))
				return mid;
			else
				return ret;

		}else {
			return findMaxDepth(scanner, min, (mid - 1));
		}
	}

	private int findMaxDepth(Scanner scanner) {
		int origBatchSize = scanner.getBatchSize();
		scanner.setBatchSize(100);
		int depth = findMaxDepth(scanner, 0, 64, 999);
		scanner.setBatchSize(origBatchSize);
		return depth;
	}

	private Map.Entry<Key, Value> findCount(Map.Entry<Key, Value> entry, Iterator<Map.Entry<Key, Value>> iterator, FileCount.CountValue cv) {
		Key key = entry.getKey();
		Text currentRow = key.getRow();
		if ((key.compareColumnQualifier(QueryUtil.COUNTS_COLQ)) == 0)
			cv.set(entry.getValue());

		while (iterator.hasNext()) {
			entry = iterator.next();
			(entriesScanned)++;
			key = entry.getKey();
			if ((key.compareRow(currentRow)) != 0)
				return entry;

			if (((key.compareColumnFamily(QueryUtil.DIR_COLF)) == 0) && ((key.compareColumnQualifier(QueryUtil.COUNTS_COLQ)) == 0)) {
				cv.set(entry.getValue());
			}
		} 
		return null;
	}

	private Map.Entry<Key, Value> consumeRow(Map.Entry<Key, Value> entry, Iterator<Map.Entry<Key, Value>> iterator) {
		Key key = entry.getKey();
		Text currentRow = key.getRow();
		while (iterator.hasNext()) {
			entry = iterator.next();
			(entriesScanned)++;
			key = entry.getKey();
			if ((key.compareRow(currentRow)) != 0)
				return entry;

		} 
		return null;
	}

	private String extractDir(Key key) {
		String row = key.getRowData().toString();
		return row.substring(3, row.lastIndexOf('/'));
	}

	private Mutation createMutation(int depth, String dir, FileCount.CountValue countVal) {
		Mutation m = new Mutation(String.format("%03d%s", depth, dir));
		m.put(QueryUtil.DIR_COLF, QueryUtil.COUNTS_COLQ, opts.visibility, countVal.toValue());
		return m;
	}

	private void calculateCounts(Scanner scanner, int depth, BatchWriter batchWriter) throws Exception {
		scanner.setRange(new Range(String.format("%03d", depth), true, String.format("%03d", (depth + 1)), false));
		FileCount.CountValue countVal = new FileCount.CountValue();
		Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
		String currentDir = null;
		Map.Entry<Key, Value> entry = null;
		if (iterator.hasNext()) {
			entry = iterator.next();
			(entriesScanned)++;
		}
		while (entry != null) {
			Key key = entry.getKey();
			String dir = extractDir(key);
			if (currentDir == null) {
				currentDir = dir;
			}else
				if (!(currentDir.equals(dir))) {
					batchWriter.addMutation(createMutation((depth - 1), currentDir, countVal));
					(inserts)++;
					currentDir = dir;
					countVal.clear();
				}

			if ((key.compareColumnFamily(QueryUtil.DIR_COLF)) == 0) {
				FileCount.CountValue tmpCount = new FileCount.CountValue();
				entry = findCount(entry, iterator, tmpCount);
				if (((tmpCount.dirCount) == 0) && ((tmpCount.fileCount) == 0)) {
					Mutation m = new Mutation(key.getRow());
					m.put(QueryUtil.DIR_COLF, QueryUtil.COUNTS_COLQ, opts.visibility, tmpCount.toValue());
					batchWriter.addMutation(m);
					(inserts)++;
				}
				countVal.incrementRecursive(tmpCount);
				countVal.incrementDirs();
			}else {
				entry = consumeRow(entry, iterator);
				countVal.incrementFiles();
			}
		} 
		if (currentDir != null) {
			batchWriter.addMutation(createMutation((depth - 1), currentDir, countVal));
			(inserts)++;
		}
	}

	FileCount(FileCount.Opts opts, ScannerOpts scanOpts, BatchWriterOpts bwOpts) throws Exception {
		this.opts = opts;
		this.scanOpts = scanOpts;
		this.bwOpts = bwOpts;
	}

	public void run() throws Exception {
		entriesScanned = 0;
		inserts = 0;
		Connector conn = opts.getConnector();
		Scanner scanner = conn.createScanner(opts.tableName, opts.auths);
		scanner.setBatchSize(scanOpts.scanBatchSize);
		BatchWriter bw = conn.createBatchWriter(opts.tableName, bwOpts.getBatchWriterConfig());
		long t1 = System.currentTimeMillis();
		int depth = findMaxDepth(scanner);
		long t2 = System.currentTimeMillis();
		for (int d = depth; d > 0; d--) {
			calculateCounts(scanner, d, bw);
			bw.flush();
		}
		bw.close();
		long t3 = System.currentTimeMillis();
		System.out.printf("Max depth              : %d%n", depth);
		System.out.printf("Time to find max depth : %,d ms%n", (t2 - t1));
		System.out.printf("Time to compute counts : %,d ms%n", (t3 - t2));
		System.out.printf("Entries scanned        : %,d %n", entriesScanned);
		System.out.printf("Counts inserted        : %,d %n", inserts);
	}

	public static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--vis", description = "use a given visibility for the new counts", converter = ClientOpts.VisibilityConverter.class)
		ColumnVisibility visibility = new ColumnVisibility();
	}

	public static void main(String[] args) throws Exception {
		FileCount.Opts opts = new FileCount.Opts();
		ScannerOpts scanOpts = new ScannerOpts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		String programName = FileCount.class.getName();
		opts.parseArgs(programName, args, scanOpts, bwOpts);
		FileCount fileCount = new FileCount(opts, scanOpts, bwOpts);
		fileCount.run();
	}
}

