package org.apache.accumulo.examples.simple.client;


import com.beust.jcommander.Parameter;
import java.io.PrintStream;
import java.util.Map;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;


public class TracingExample {
	private static final String DEFAULT_TABLE_NAME = "test";

	static class Opts extends ClientOnDefaultTable {
		@Parameter(names = { "-C", "--createtable" }, description = "create table before doing anything")
		boolean createtable = false;

		@Parameter(names = { "-D", "--deletetable" }, description = "delete table when finished")
		boolean deletetable = false;

		@Parameter(names = { "-c", "--create" }, description = "create entries before any deletes")
		boolean createEntries = false;

		@Parameter(names = { "-r", "--read" }, description = "read entries after any creates/deletes")
		boolean readEntries = false;

		public Opts() {
			super(TracingExample.DEFAULT_TABLE_NAME);
			auths = new Authorizations();
		}
	}

	public void enableTracing(TracingExample.Opts opts) throws Exception {
		DistributedTrace.enable(opts.getInstance(), new ZooReader(opts.getInstance().getZooKeepers(), 1000), "myHost", "myApp");
	}

	public void execute(TracingExample.Opts opts) throws InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		if (opts.createtable) {
			opts.getConnector().tableOperations().create(opts.getTableName());
		}
		if (opts.createEntries) {
			createEntries(opts);
		}
		if (opts.readEntries) {
			readEntries(opts);
		}
		if (opts.deletetable) {
			opts.getConnector().tableOperations().delete(opts.getTableName());
		}
	}

	private void createEntries(TracingExample.Opts opts) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		BatchWriter batchWriter = opts.getConnector().createBatchWriter(opts.getTableName(), new BatchWriterConfig());
		Mutation m = new Mutation("row");
		m.put("cf", "cq", "value");
		Trace.on("Client Write");
		batchWriter.addMutation(m);
		Span flushSpan = Trace.start("Client Flush");
		batchWriter.flush();
		flushSpan.stop();
		Trace.off();
		batchWriter.close();
	}

	private void readEntries(TracingExample.Opts opts) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		Scanner scanner = opts.getConnector().createScanner(opts.getTableName(), opts.auths);
		Span readSpan = Trace.on("Client Read");
		int numberOfEntriesRead = 0;
		for (Map.Entry<Key, Value> entry : scanner) {
			System.out.println((((entry.getKey().toString()) + " -> ") + (entry.getValue().toString())));
			++numberOfEntriesRead;
		}
		readSpan.data("Number of Entries Read", String.valueOf(numberOfEntriesRead));
		Trace.off();
	}

	public static void main(String[] args) throws Exception {
		TracingExample tracingExample = new TracingExample();
		TracingExample.Opts opts = new TracingExample.Opts();
		ScannerOpts scannerOpts = new ScannerOpts();
		opts.parseArgs(TracingExample.class.getName(), args, scannerOpts);
		tracingExample.enableTracing(opts);
		tracingExample.execute(opts);
	}
}

