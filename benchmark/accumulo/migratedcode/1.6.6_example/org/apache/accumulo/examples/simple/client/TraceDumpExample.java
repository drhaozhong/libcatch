package org.apache.accumulo.examples.simple.client;


import com.beust.jcommander.Parameter;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;


public class TraceDumpExample {
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
		Scanner scanner = opts.getConnector().createScanner(opts.getTableName(), opts.auths);
		scanner.setRange(new Range(new Text(opts.traceId)));
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		TraceDumpExample traceDumpExample = new TraceDumpExample();
		TraceDumpExample.Opts opts = new TraceDumpExample.Opts();
		ScannerOpts scannerOpts = new ScannerOpts();
		opts.parseArgs(TraceDumpExample.class.getName(), args, scannerOpts);
		traceDumpExample.dump(opts);
	}
}

