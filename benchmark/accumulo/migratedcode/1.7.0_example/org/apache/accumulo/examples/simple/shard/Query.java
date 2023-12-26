package org.apache.accumulo.examples.simple.shard;


import com.beust.jcommander.Parameter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.cli.BatchScannerOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;


public class Query {
	static class Opts extends ClientOnRequiredTable {
		@Parameter(description = " term { <term> ... }")
		List<String> terms = new ArrayList<String>();
	}

	public static List<String> query(BatchScanner bs, List<String> terms) {
		Text[] columns = new Text[terms.size()];
		int i = 0;
		for (String term : terms) {
			columns[(i++)] = new Text(term);
		}
		IteratorSetting ii = new IteratorSetting(20, "ii", IntersectingIterator.class);
		IntersectingIterator.setColumnFamilies(ii, columns);
		bs.addScanIterator(ii);
		bs.setRanges(Collections.singleton(new Range()));
		List<String> result = new ArrayList<String>();
		for (Map.Entry<Key, Value> entry : bs) {
			result.add(entry.getKey().getColumnQualifier().toString());
		}
		return result;
	}

	public static void main(String[] args) throws Exception {
		Query.Opts opts = new Query.Opts();
		BatchScannerOpts bsOpts = new BatchScannerOpts();
		opts.parseArgs(Query.class.getName(), args, bsOpts);
		Connector conn = opts.getConnector();
		BatchScanner bs = conn.createBatchScanner(opts.getTableName(), opts.auths, bsOpts.scanThreads);
		bs.setTimeout(bsOpts.scanTimeout, TimeUnit.MILLISECONDS);
		for (String entry : Query.query(bs, opts.terms))
			System.out.println(("  " + entry));

	}
}

