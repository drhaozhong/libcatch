package org.apache.accumulo.examples.simple.helloworld;


import com.beust.jcommander.Parameter;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
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
import org.apache.log4j.Logger;


public class ReadData {
	private static final Logger log = Logger.getLogger(ReadData.class);

	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--startKey")
		String startKey;

		@Parameter(names = "--endKey")
		String endKey;
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		ReadData.Opts opts = new ReadData.Opts();
		ScannerOpts scanOpts = new ScannerOpts();
		opts.parseArgs(ReadData.class.getName(), args, scanOpts);
		Connector connector = opts.getConnector();
		Scanner scan = connector.createScanner(opts.tableName, opts.auths);
		scan.setBatchSize(scanOpts.scanBatchSize);
		Key start = null;
		if ((opts.startKey) != null)
			start = new Key(new Text(opts.startKey));

		Key end = null;
		if ((opts.endKey) != null)
			end = new Key(new Text(opts.endKey));

		scan.setRange(new Range(start, end));
		Iterator<Map.Entry<Key, Value>> iter = scan.iterator();
		while (iter.hasNext()) {
			Map.Entry<Key, Value> e = iter.next();
			Text colf = e.getKey().getColumnFamily();
			Text colq = e.getKey().getColumnQualifier();
			ReadData.log.trace(((((("row: " + (e.getKey().getRow())) + ", colf: ") + colf) + ", colq: ") + colq));
			ReadData.log.trace((", value: " + (e.getValue().toString())));
		} 
	}
}

