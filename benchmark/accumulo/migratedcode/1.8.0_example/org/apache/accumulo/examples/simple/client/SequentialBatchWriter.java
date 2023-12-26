package org.apache.accumulo.examples.simple.client;


import com.beust.jcommander.Parameter;
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
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;


public class SequentialBatchWriter {
	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--start")
		long start = 0;

		@Parameter(names = "--num", required = true)
		long num = 0;

		@Parameter(names = "--size", required = true, description = "size of the value to write")
		int valueSize = 0;

		@Parameter(names = "--vis", converter = ClientOpts.VisibilityConverter.class)
		ColumnVisibility vis = new ColumnVisibility();
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableNotFoundException {
		SequentialBatchWriter.Opts opts = new SequentialBatchWriter.Opts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(SequentialBatchWriter.class.getName(), args, bwOpts);
		Connector connector = opts.getConnector();
		BatchWriter bw = connector.createBatchWriter(opts.getTableName(), bwOpts.getBatchWriterConfig());
		long end = (opts.start) + (opts.num);
		for (long i = opts.start; i < end; i++) {
			Mutation m = RandomBatchWriter.createMutation(i, opts.valueSize, opts.vis);
			bw.addMutation(m);
		}
		bw.close();
	}
}

