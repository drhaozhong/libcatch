package org.apache.accumulo.examples.simple.client;


import com.beust.jcommander.Parameter;
import java.io.PrintStream;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.tracer.TraceDump;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TraceDumpExample {
	private static final Logger log = LoggerFactory.getLogger(TraceDumpExample.class);

	static class Opts extends ClientOnDefaultTable {
		public Opts() {
			super("trace");
		}

		@Parameter(names = { "--traceid" }, description = "The hex string id of a given trace, for example 16cfbbd7beec4ae3")
		public String traceId = "";
	}

	public void dump(TraceDumpExample.Opts opts) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		if (opts.traceId.isEmpty()) {
			throw new IllegalArgumentException("--traceid option is required");
		}
		final Connector conn = opts.getConnector();
		final String principal = opts.getPrincipal();
		final String table = opts.getTableName();
		if (!(conn.securityOperations().hasTablePermission(principal, table, TablePermission.READ))) {
			conn.securityOperations().grantTablePermission(principal, table, TablePermission.READ);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
			while (!(conn.securityOperations().hasTablePermission(principal, table, TablePermission.READ))) {
				TraceDumpExample.log.info("{} didn't propagate read permission on {}", principal, table);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(e);
				}
			} 
		}
		Scanner scanner = conn.createScanner(table, opts.auths);
		scanner.setRange(new Range(new Text(opts.traceId)));
		TraceDump.printTrace(scanner, new TraceDump.Printer() {
			@Override
			public void print(String line) {
				System.out.println(line);
			}
		});
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		TraceDumpExample traceDumpExample = new TraceDumpExample();
		TraceDumpExample.Opts opts = new TraceDumpExample.Opts();
		ScannerOpts scannerOpts = new ScannerOpts();
		opts.parseArgs(TraceDumpExample.class.getName(), args, scannerOpts);
		traceDumpExample.dump(opts);
	}
}

