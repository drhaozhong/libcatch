package org.apache.accumulo.examples.simple.client;


import com.beust.jcommander.Parameter;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
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

	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--num", required = true)
		int num = 0;

		@Parameter(names = "--min")
		long min = 0;

		@Parameter(names = "--max")
		long max = Long.MAX_VALUE;

		@Parameter(names = "--size", required = true, description = "size of the value to write")
		int size = 0;

		@Parameter(names = "--vis", converter = ClientOpts.VisibilityConverter.class)
		ColumnVisibility visiblity = new ColumnVisibility("");

		@Parameter(names = "--seed", description = "seed for pseudo-random number generator")
		Long seed = null;
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		RandomBatchWriter.Opts opts = new RandomBatchWriter.Opts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(RandomBatchWriter.class.getName(), args, bwOpts);
		if (((opts.max) - (opts.min)) < (opts.num)) {
			System.err.println(String.format("You must specify a min and a max that allow for at least num possible values. For example, you requested %d rows, but a min of %d and a max of %d only allows for %d rows.", opts.num, opts.min, opts.max, ((opts.max) - (opts.min))));
			System.exit(1);
		}
		Random r;
		if ((opts.seed) == null)
			r = new Random();
		else {
			r = new Random(opts.seed);
		}
		Connector connector = opts.getConnector();
		BatchWriter bw = connector.createBatchWriter(opts.tableName, bwOpts.getBatchWriterConfig());
		ColumnVisibility cv = opts.visiblity;
		HashSet<Long> rowids = new HashSet<Long>(opts.num);
		while ((rowids.size()) < (opts.num)) {
			rowids.add((((Math.abs(r.nextLong())) % ((opts.max) - (opts.min))) + (opts.min)));
		} 
		for (long rowid : rowids) {
			Mutation m = RandomBatchWriter.createMutation(rowid, opts.size, cv);
			bw.addMutation(m);
		}
		try {
			bw.close();
		} catch (MutationsRejectedException e) {
			if ((e.getAuthorizationFailuresMap().size()) > 0) {
				HashMap<String, Set<SecurityErrorCode>> tables = new HashMap<String, Set<SecurityErrorCode>>();
				for (Map.Entry<KeyExtent, Set<SecurityErrorCode>> ke : e.getAuthorizationFailuresMap().entrySet()) {
					Set<SecurityErrorCode> secCodes = tables.get(ke.getKey().getTableId().toString());
					if (secCodes == null) {
						secCodes = new HashSet<SecurityErrorCode>();
						tables.put(ke.getKey().getTableId().toString(), secCodes);
					}
					secCodes.addAll(ke.getValue());
				}
				System.err.println(("ERROR : Not authorized to write to tables : " + tables));
			}
			if ((e.getConstraintViolationSummaries().size()) > 0) {
				System.err.println(("ERROR : Constraint violations occurred : " + (e.getConstraintViolationSummaries())));
			}
			System.exit(1);
		}
	}
}

