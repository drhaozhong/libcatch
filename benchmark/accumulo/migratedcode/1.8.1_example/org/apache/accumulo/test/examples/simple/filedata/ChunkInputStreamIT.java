package org.apache.accumulo.test.examples.simple.filedata;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.examples.simple.filedata.ChunkInputStream;
import org.apache.accumulo.examples.simple.filedata.FileDataIngest;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.hadoop.io.Text;
import org.junit.Assert;


public class ChunkInputStreamIT extends AccumuloClusterHarness {
	private static final Authorizations AUTHS = new Authorizations("A", "B", "C", "D");

	private Connector conn;

	private String tableName;

	private List<Map.Entry<Key, Value>> data;

	private List<Map.Entry<Key, Value>> baddata;

	private List<Map.Entry<Key, Value>> multidata;

	@org.junit.Before
	public void setupInstance() throws Exception {
		conn = getConnector();
		tableName = getUniqueNames(1)[0];
		conn.securityOperations().changeUserAuthorizations(conn.whoami(), ChunkInputStreamIT.AUTHS);
	}

	@org.junit.Before
	public void setupData() {
		data = new ArrayList<>();
		ChunkInputStreamIT.addData(data, "a", "refs", "id\u0000ext", "A&B", "ext");
		ChunkInputStreamIT.addData(data, "a", "refs", "id\u0000name", "A&B", "name");
		ChunkInputStreamIT.addData(data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamIT.addData(data, "a", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamIT.addData(data, "b", "refs", "id\u0000ext", "A&B", "ext");
		ChunkInputStreamIT.addData(data, "b", "refs", "id\u0000name", "A&B", "name");
		ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
		ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
		ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 1, "B&C", "");
		ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 1, "D", "");
		ChunkInputStreamIT.addData(data, "c", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamIT.addData(data, "c", "~chunk", 100, 1, "A&B", "asdfjkl;");
		ChunkInputStreamIT.addData(data, "c", "~chunk", 100, 2, "A&B", "");
		ChunkInputStreamIT.addData(data, "d", "~chunk", 100, 0, "A&B", "");
		ChunkInputStreamIT.addData(data, "e", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamIT.addData(data, "e", "~chunk", 100, 1, "A&B", "");
		baddata = new ArrayList<>();
		ChunkInputStreamIT.addData(baddata, "a", "~chunk", 100, 0, "A", "asdfjkl;");
		ChunkInputStreamIT.addData(baddata, "b", "~chunk", 100, 0, "B", "asdfjkl;");
		ChunkInputStreamIT.addData(baddata, "b", "~chunk", 100, 2, "C", "");
		ChunkInputStreamIT.addData(baddata, "c", "~chunk", 100, 0, "D", "asdfjkl;");
		ChunkInputStreamIT.addData(baddata, "c", "~chunk", 100, 2, "E", "");
		ChunkInputStreamIT.addData(baddata, "d", "~chunk", 100, 0, "F", "asdfjkl;");
		ChunkInputStreamIT.addData(baddata, "d", "~chunk", 100, 1, "G", "");
		ChunkInputStreamIT.addData(baddata, "d", "~zzzzz", "colq", "H", "");
		ChunkInputStreamIT.addData(baddata, "e", "~chunk", 100, 0, "I", "asdfjkl;");
		ChunkInputStreamIT.addData(baddata, "e", "~chunk", 100, 1, "J", "");
		ChunkInputStreamIT.addData(baddata, "e", "~chunk", 100, 2, "I", "asdfjkl;");
		ChunkInputStreamIT.addData(baddata, "f", "~chunk", 100, 2, "K", "asdfjkl;");
		ChunkInputStreamIT.addData(baddata, "g", "~chunk", 100, 0, "L", "");
		multidata = new ArrayList<>();
		ChunkInputStreamIT.addData(multidata, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamIT.addData(multidata, "a", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamIT.addData(multidata, "a", "~chunk", 200, 0, "B&C", "asdfjkl;");
		ChunkInputStreamIT.addData(multidata, "b", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamIT.addData(multidata, "b", "~chunk", 200, 0, "B&C", "asdfjkl;");
		ChunkInputStreamIT.addData(multidata, "b", "~chunk", 200, 1, "B&C", "asdfjkl;");
		ChunkInputStreamIT.addData(multidata, "c", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamIT.addData(multidata, "c", "~chunk", 100, 1, "B&C", "");
	}

	static void addData(List<Map.Entry<Key, Value>> data, String row, String cf, String cq, String vis, String value) {
		data.add(new KeyValue(new Key(new Text(row), new Text(cf), new Text(cq), new Text(vis)), value.getBytes()));
	}

	static void addData(List<Map.Entry<Key, Value>> data, String row, String cf, int chunkSize, int chunkCount, String vis, String value) {
		Text chunkCQ = new Text(FileDataIngest.intToBytes(chunkSize));
		chunkCQ.append(FileDataIngest.intToBytes(chunkCount), 0, 4);
		data.add(new KeyValue(new Key(new Text(row), new Text(cf), chunkCQ, new Text(vis)), value.getBytes()));
	}

	@org.junit.Test
	public void testWithAccumulo() throws IOException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		conn.tableOperations().create(tableName);
		BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
		for (Map.Entry<Key, Value> e : data) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		Scanner scan = conn.createScanner(tableName, ChunkInputStreamIT.AUTHS);
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(scan.iterator());
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 8);
		Assert.assertEquals(new String(b, 0, read), "asdfjkl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 10);
		Assert.assertEquals(new String(b, 0, read), "qwertyuiop");
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B, B&C, D]");
		cis.close();
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 16);
		Assert.assertEquals(new String(b, 0, read), "asdfjkl;asdfjkl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.close();
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 8);
		Assert.assertEquals(new String(b, 0, read), "asdfjkl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		Assert.assertFalse(pi.hasNext());
	}
}

