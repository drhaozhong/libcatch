package org.apache.accumulo.examples.simple.sample;


import com.google.common.collect.ImmutableMap;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.simple.client.RandomBatchWriter;


public class SampleExample {
	static final CompactionStrategyConfig NO_SAMPLE_STRATEGY = new CompactionStrategyConfig("org.apache.accumulo.tserver.compaction.strategies.ConfigurableCompactionStrategy").setOptions(Collections.singletonMap("SF_NO_SAMPLE", ""));

	static class Opts extends ClientOnDefaultTable {
		public Opts() {
			super("sampex");
		}
	}

	public static void main(String[] args) throws Exception {
		SampleExample.Opts opts = new SampleExample.Opts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(RandomBatchWriter.class.getName(), args, bwOpts);
		Connector conn = opts.getConnector();
		if (!(conn.tableOperations().exists(opts.getTableName()))) {
			conn.tableOperations().create(opts.getTableName());
		}else {
			System.out.println("Table exists, not doing anything.");
			return;
		}
		BatchWriter bw = conn.createBatchWriter(opts.getTableName(), bwOpts.getBatchWriterConfig());
		bw.addMutation(SampleExample.createMutation("9225", "abcde", "file://foo.txt"));
		bw.addMutation(SampleExample.createMutation("8934", "accumulo scales", "file://accumulo_notes.txt"));
		bw.addMutation(SampleExample.createMutation("2317", "milk, eggs, bread, parmigiano-reggiano", "file://groceries/9/txt"));
		bw.addMutation(SampleExample.createMutation("3900", "EC2 ate my homework", "file://final_project.txt"));
		bw.flush();
		SamplerConfiguration sc1 = new SamplerConfiguration(RowSampler.class.getName());
		sc1.setOptions(ImmutableMap.of("hasher", "murmur3_32", "modulus", "3"));
		conn.tableOperations().setSamplerConfiguration(opts.getTableName(), sc1);
		Scanner scanner = conn.createScanner(opts.getTableName(), Authorizations.EMPTY);
		System.out.println("Scanning all data :");
		SampleExample.print(scanner);
		System.out.println();
		System.out.println("Scanning with sampler configuration.  Data was written before sampler was set on table, scan should fail.");
		scanner.setSamplerConfiguration(sc1);
		try {
			SampleExample.print(scanner);
		} catch (SampleNotPresentException e) {
			System.out.println("  Saw sample not present exception as expected.");
		}
		System.out.println();
		conn.tableOperations().compact(opts.getTableName(), new CompactionConfig().setCompactionStrategy(SampleExample.NO_SAMPLE_STRATEGY));
		System.out.println("Scanning after compaction (compaction should have created sample data) : ");
		SampleExample.print(scanner);
		System.out.println();
		bw.addMutation(SampleExample.createMutation("2317", "milk, eggs, bread, parmigiano-reggiano, butter", "file://groceries/9/txt"));
		bw.close();
		System.out.println("Scanning sample after updating content for docId 2317 (should see content change in sample data) : ");
		SampleExample.print(scanner);
		System.out.println();
		SamplerConfiguration sc2 = new SamplerConfiguration(RowSampler.class.getName());
		sc2.setOptions(ImmutableMap.of("hasher", "murmur3_32", "modulus", "2"));
		conn.tableOperations().setSamplerConfiguration(opts.getTableName(), sc2);
		conn.tableOperations().compact(opts.getTableName(), new CompactionConfig().setCompactionStrategy(SampleExample.NO_SAMPLE_STRATEGY));
		System.out.println("Scanning with old sampler configuration.  Sample data was created using new configuration with a compaction.  Scan should fail.");
		try {
			SampleExample.print(scanner);
		} catch (SampleNotPresentException e) {
			System.out.println("  Saw sample not present exception as expected ");
		}
		System.out.println();
		scanner.setSamplerConfiguration(sc2);
		System.out.println("Scanning with new sampler configuration : ");
		SampleExample.print(scanner);
		System.out.println();
	}

	private static void print(Scanner scanner) {
		for (Map.Entry<Key, Value> entry : scanner) {
			System.out.println(((("  " + (entry.getKey())) + " ") + (entry.getValue())));
		}
	}

	private static Mutation createMutation(String docId, String content, String url) {
		Mutation m = new Mutation(docId);
		m.put("doc", "context", content);
		m.put("doc", "url", url);
		return m;
	}
}

