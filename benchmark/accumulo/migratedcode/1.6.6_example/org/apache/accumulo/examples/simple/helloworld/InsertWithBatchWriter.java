package org.apache.accumulo.examples.simple.helloworld;


import java.io.PrintStream;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;


public class InsertWithBatchWriter {
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableExistsException, TableNotFoundException {
		ClientOnRequiredTable opts = new ClientOnRequiredTable();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(InsertWithBatchWriter.class.getName(), args, bwOpts);
		Connector connector = opts.getConnector();
		MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(bwOpts.getBatchWriterConfig());
		if (!(connector.tableOperations().exists(opts.tableName))) {
		}
		BatchWriter bw = mtbw.getBatchWriter(opts.tableName);
		Text colf = new Text("colfam");
		System.out.println("writing ...");
		for (int i = 0; i < 10000; i++) {
			Mutation m = new Mutation(new Text(String.format("row_%d", i)));
			for (int j = 0; j < 5; j++) {
				m.put(colf, new Text(String.format("colqual_%d", j)), new Value(String.format("value_%d_%d", i, j).getBytes()));
			}
			bw.addMutation(m);
			if ((i % 100) == 0)
				System.out.println(i);

		}
		mtbw.close();
	}
}

