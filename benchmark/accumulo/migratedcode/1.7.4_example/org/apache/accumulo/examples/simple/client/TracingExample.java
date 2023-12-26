package org.apache.accumulo.examples.simple.client;


import com.beust.jcommander.Parameter;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TracingExample {
	private static final Logger log = LoggerFactory.getLogger(TracingExample.class);

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
		DistributedTrace.enable("myHost", "myApp");
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
		TraceScope scope = Trace.startSpan("Client Write", Sampler.ALWAYS);
		System.out.println(("TraceID: " + (Long.toHexString(scope.getSpan().getTraceId()))));
		BatchWriter batchWriter = opts.getConnector().createBatchWriter(opts.getTableName(), new BatchWriterConfig());
		Mutation m = new Mutation("row");
		m.put("cf", "cq", "value");
		batchWriter.addMutation(m);
		scope.getSpan().addTimelineAnnotation("Initiating Flush");
		batchWriter.flush();
		batchWriter.close();
		scope.close();
	}

	private void readEntries(TracingExample.Opts opts) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		Scanner scanner = opts.getConnector().createScanner(opts.getTableName(), opts.auths);
		TraceScope readScope = Trace.startSpan("Client Read", Sampler.ALWAYS);
		System.out.println(("TraceID: " + (Long.toHexString(readScope.getSpan().getTraceId()))));
		int numberOfEntriesRead = 0;
		for (Map.Entry<Key, Value> entry : scanner) {
			System.out.println((((entry.getKey().toString()) + " -> ") + (entry.getValue().toString())));
			++numberOfEntriesRead;
		}
		readScope.getSpan().addKVAnnotation("Number of Entries Read".getBytes(StandardCharsets.UTF_8), String.valueOf(numberOfEntriesRead).getBytes(StandardCharsets.UTF_8));
		readScope.close();
	}

	public static void main(String[] args) throws Exception {
		try {
			TracingExample tracingExample = new TracingExample();
			TracingExample.Opts opts = new TracingExample.Opts();
			ScannerOpts scannerOpts = new ScannerOpts();
			opts.parseArgs(TracingExample.class.getName(), args, scannerOpts);
			tracingExample.enableTracing(opts);
			tracingExample.execute(opts);
		} catch (Exception e) {
			TracingExample.log.error("Caught exception running TraceExample", e);
			System.exit(1);
		}
	}
}

