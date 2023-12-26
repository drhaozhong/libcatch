package org.apache.accumulo.examples.simple.dirlist;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;


public class CountTest extends TestCase {
	{
		try {
			Connector conn = new MockInstance("counttest").getConnector("root", "".getBytes());
			conn.tableOperations().create("dirlisttable");
			BatchWriter bw = conn.createBatchWriter("dirlisttable", 1000000L, 100L, 1);
			ColumnVisibility cv = new ColumnVisibility();
			bw.addMutation(Ingest.buildMutation(cv, "/local", true, false, true, 272, 12345, null));
			bw.addMutation(Ingest.buildMutation(cv, "/local/user1", true, false, true, 272, 12345, null));
			bw.addMutation(Ingest.buildMutation(cv, "/local/user2", true, false, true, 272, 12345, null));
			bw.addMutation(Ingest.buildMutation(cv, "/local/file", false, false, false, 1024, 12345, null));
			bw.addMutation(Ingest.buildMutation(cv, "/local/file", false, false, false, 1024, 23456, null));
			bw.addMutation(Ingest.buildMutation(cv, "/local/user1/file1", false, false, false, 2024, 12345, null));
			bw.addMutation(Ingest.buildMutation(cv, "/local/user1/file2", false, false, false, 1028, 23456, null));
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void test() throws Exception {
		Scanner scanner = new MockInstance("counttest").getConnector("root", "".getBytes()).createScanner("dirlisttable", new Authorizations());
		scanner.fetchColumn(new Text("dir"), new Text("counts"));
		TestCase.assertFalse(scanner.iterator().hasNext());
		FileCount fc = new FileCount("counttest", null, "root", "", "dirlisttable", "", "", true);
		fc.run();
		ArrayList<Pair<String, String>> expected = new ArrayList<Pair<String, String>>();
		expected.add(new Pair<String, String>(QueryUtil.getRow("").toString(), "1,0,3,3"));
		expected.add(new Pair<String, String>(QueryUtil.getRow("/local").toString(), "2,1,2,3"));
		expected.add(new Pair<String, String>(QueryUtil.getRow("/local/user1").toString(), "0,2,0,2"));
		expected.add(new Pair<String, String>(QueryUtil.getRow("/local/user2").toString(), "0,0,0,0"));
		int i = 0;
		for (Map.Entry<Key, Value> e : scanner) {
			TestCase.assertEquals(e.getKey().getRow().toString(), expected.get(i).getFirst());
			TestCase.assertEquals(e.getValue().toString(), expected.get(i).getSecond());
			i++;
		}
		TestCase.assertEquals(i, expected.size());
	}
}

