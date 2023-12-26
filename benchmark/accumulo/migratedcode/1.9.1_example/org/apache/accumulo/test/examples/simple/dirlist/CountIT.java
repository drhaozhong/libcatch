package org.apache.accumulo.test.examples.simple.dirlist;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.examples.simple.dirlist.FileCount;
import org.apache.accumulo.examples.simple.dirlist.Ingest;
import org.apache.accumulo.examples.simple.dirlist.QueryUtil;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.junit.Assert;


public class CountIT extends ConfigurableMacBase {
	private Connector conn;

	private String tableName;

	@org.junit.Before
	public void setupInstance() throws Exception {
		tableName = getUniqueNames(1)[0];
		conn = getConnector();
		conn.tableOperations().create(tableName);
		BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
		ColumnVisibility cv = new ColumnVisibility();
		bw.addMutation(Ingest.buildMutation(cv, "/local", true, false, true, 272, 12345, null));
		bw.addMutation(Ingest.buildMutation(cv, "/local/user1", true, false, true, 272, 12345, null));
		bw.addMutation(Ingest.buildMutation(cv, "/local/user2", true, false, true, 272, 12345, null));
		bw.addMutation(Ingest.buildMutation(cv, "/local/file", false, false, false, 1024, 12345, null));
		bw.addMutation(Ingest.buildMutation(cv, "/local/file", false, false, false, 1024, 23456, null));
		bw.addMutation(Ingest.buildMutation(cv, "/local/user1/file1", false, false, false, 2024, 12345, null));
		bw.addMutation(Ingest.buildMutation(cv, "/local/user1/file2", false, false, false, 1028, 23456, null));
		bw.close();
	}

	@org.junit.Test
	public void test() throws Exception {
		Scanner scanner = conn.createScanner(tableName, new Authorizations());
		scanner.fetchColumn(new Text("dir"), new Text("counts"));
		Assert.assertFalse(scanner.iterator().hasNext());
		FileCount.Opts opts = new FileCount.Opts();
		ScannerOpts scanOpts = new ScannerOpts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.instance = conn.getInstance().getInstanceName();
		opts.zookeepers = conn.getInstance().getZooKeepers();
		opts.setTableName(tableName);
		opts.setPrincipal(conn.whoami());
		opts.setPassword(new ClientOpts.Password(ConfigurableMacBase.ROOT_PASSWORD));
		FileCount fc = new FileCount(opts, scanOpts, bwOpts);
		fc.run();
		ArrayList<Pair<String, String>> expected = new ArrayList<>();
		expected.add(new Pair<>(QueryUtil.getRow("").toString(), "1,0,3,3"));
		expected.add(new Pair<>(QueryUtil.getRow("/local").toString(), "2,1,2,3"));
		expected.add(new Pair<>(QueryUtil.getRow("/local/user1").toString(), "0,2,0,2"));
		expected.add(new Pair<>(QueryUtil.getRow("/local/user2").toString(), "0,0,0,0"));
		int i = 0;
		for (Map.Entry<Key, Value> e : scanner) {
			Assert.assertEquals(e.getKey().getRow().toString(), expected.get(i).getFirst());
			Assert.assertEquals(e.getValue().toString(), expected.get(i).getSecond());
			i++;
		}
		Assert.assertEquals(i, expected.size());
	}
}

