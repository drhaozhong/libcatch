package org.apache.accumulo.examples.simple.shard;


import com.beust.jcommander.Parameter;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;


public class Index {
	static Text genPartition(int partition) {
		return new Text(String.format("%08x", Math.abs(partition)));
	}

	public static void index(int numPartitions, Text docId, String doc, String splitRegex, BatchWriter bw) throws Exception {
		String[] tokens = doc.split(splitRegex);
		Text partition = Index.genPartition(((doc.hashCode()) % numPartitions));
		Mutation m = new Mutation(partition);
		HashSet<String> tokensSeen = new HashSet<String>();
		for (String token : tokens) {
			token = token.toLowerCase();
			if (!(tokensSeen.contains(token))) {
				tokensSeen.add(token);
				m.put(new Text(token), docId, new Value(new byte[0]));
			}
		}
		if ((m.size()) > 0)
			bw.addMutation(m);

	}

	public static void index(int numPartitions, File src, String splitRegex, BatchWriter bw) throws Exception {
		if (src.isDirectory()) {
			File[] files = src.listFiles();
			if (files != null) {
				for (File child : files) {
					Index.index(numPartitions, child, splitRegex, bw);
				}
			}
		}else {
			FileReader fr = new FileReader(src);
			StringBuilder sb = new StringBuilder();
			char[] data = new char[4096];
			int len;
			while ((len = fr.read(data)) != (-1)) {
				sb.append(data, 0, len);
			} 
			fr.close();
			Index.index(numPartitions, new Text(src.getAbsolutePath()), sb.toString(), splitRegex, bw);
		}
	}

	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--partitions", required = true, description = "the number of shards to create")
		int partitions;

		@Parameter(required = true, description = "<file> { <file> ... }")
		List<String> files = new ArrayList<String>();
	}

	public static void main(String[] args) throws Exception {
		Index.Opts opts = new Index.Opts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(Index.class.getName(), args, bwOpts);
		String splitRegex = "\\W+";
		BatchWriter bw = opts.getConnector().createBatchWriter(opts.getTableName(), bwOpts.getBatchWriterConfig());
		for (String filename : opts.files) {
			Index.index(opts.partitions, new File(filename), splitRegex, bw);
		}
		bw.close();
	}
}

