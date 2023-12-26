package org.apache.accumulo.examples.simple.mapreduce.bulk;


import com.beust.jcommander.Parameter;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VerifyIngest {
	private static final Logger log = LoggerFactory.getLogger(VerifyIngest.class);

	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--start-row")
		int startRow = 0;

		@Parameter(names = "--count", required = true, description = "number of rows to verify")
		int numRows = 0;
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		VerifyIngest.Opts opts = new VerifyIngest.Opts();
		opts.parseArgs(VerifyIngest.class.getName(), args);
		Connector connector = opts.getConnector();
		Scanner scanner = connector.createScanner(opts.getTableName(), opts.auths);
		scanner.setRange(new Range(new Text(String.format("row_%08d", opts.startRow)), null));
		Iterator<Map.Entry<Key, Value>> si = scanner.iterator();
		boolean ok = true;
		for (int i = opts.startRow; i < (opts.numRows); i++) {
			if (si.hasNext()) {
				Map.Entry<Key, Value> entry = si.next();
				if (!(entry.getKey().getRow().toString().equals(String.format("row_%08d", i)))) {
					VerifyIngest.log.error(((("unexpected row key " + (entry.getKey().getRow().toString())) + " expected ") + (String.format("row_%08d", i))));
					ok = false;
				}
				if (!(entry.getValue().toString().equals(String.format("value_%08d", i)))) {
					VerifyIngest.log.error(((("unexpected value " + (entry.getValue().toString())) + " expected ") + (String.format("value_%08d", i))));
					ok = false;
				}
			}else {
				VerifyIngest.log.error(("no more rows, expected " + (String.format("row_%08d", i))));
				ok = false;
				break;
			}
		}
		if (ok)
			System.out.println("OK");

	}
}

