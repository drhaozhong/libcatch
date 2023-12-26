package org.apache.accumulo.examples.simple.shard;


import com.beust.jcommander.Parameter;
import java.util.Map;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;


public class Reverse {
	static class Opts extends ClientOpts {
		@Parameter(names = "--shardTable")
		String shardTable = "shard";

		@Parameter(names = "--doc2Term")
		String doc2TermTable = "doc2Term";
	}

	public static void main(String[] args) throws Exception {
		Reverse.Opts opts = new Reverse.Opts();
		ScannerOpts scanOpts = new ScannerOpts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(Reverse.class.getName(), args, scanOpts, bwOpts);
		Connector conn = opts.getConnector();
		Scanner scanner = conn.createScanner(opts.shardTable, opts.auths);
		scanner.setBatchSize(scanOpts.scanBatchSize);
		BatchWriter bw = conn.createBatchWriter(opts.doc2TermTable, bwOpts.getBatchWriterConfig());
		for (Map.Entry<Key, Value> entry : scanner) {
			Key key = entry.getKey();
			Mutation m = new Mutation(key.getColumnQualifier());
			m.put(key.getColumnFamily(), new Text(), new Value(new byte[0]));
			bw.addMutation(m);
		}
		bw.close();
	}
}

