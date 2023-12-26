package org.apache.accumulo.examples.simple.filedata;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import junit.framework.TestCase;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class ChunkInputStreamTest extends TestCase {
	private static final Logger log = Logger.getLogger(ChunkInputStream.class);

	List<Map.Entry<Key, Value>> data;

	List<Map.Entry<Key, Value>> baddata;

	List<Map.Entry<Key, Value>> multidata;

	{
		data = new ArrayList<Map.Entry<Key, Value>>();
		ChunkInputStreamTest.addData(data, "a", "refs", "id\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(data, "a", "refs", "id\u0000name", "A&B", "name");
		ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(data, "b", "refs", "id\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(data, "b", "refs", "id\u0000name", "A&B", "name");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "B&C", "");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "D", "");
		ChunkInputStreamTest.addData(data, "c", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(data, "c", "~chunk", 100, 1, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(data, "c", "~chunk", 100, 2, "A&B", "");
		ChunkInputStreamTest.addData(data, "d", "~chunk", 100, 0, "A&B", "");
		ChunkInputStreamTest.addData(data, "e", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(data, "e", "~chunk", 100, 1, "A&B", "");
		baddata = new ArrayList<Map.Entry<Key, Value>>();
		ChunkInputStreamTest.addData(baddata, "a", "~chunk", 100, 0, "A", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "b", "~chunk", 100, 0, "B", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "b", "~chunk", 100, 2, "C", "");
		ChunkInputStreamTest.addData(baddata, "c", "~chunk", 100, 0, "D", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "c", "~chunk", 100, 2, "E", "");
		ChunkInputStreamTest.addData(baddata, "d", "~chunk", 100, 0, "F", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "d", "~chunk", 100, 1, "G", "");
		ChunkInputStreamTest.addData(baddata, "d", "~zzzzz", "colq", "H", "");
		ChunkInputStreamTest.addData(baddata, "e", "~chunk", 100, 0, "I", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "e", "~chunk", 100, 1, "J", "");
		ChunkInputStreamTest.addData(baddata, "e", "~chunk", 100, 2, "I", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "f", "~chunk", 100, 2, "K", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "g", "~chunk", 100, 0, "L", "");
		multidata = new ArrayList<Map.Entry<Key, Value>>();
		ChunkInputStreamTest.addData(multidata, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "a", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(multidata, "a", "~chunk", 200, 0, "B&C", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "b", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "b", "~chunk", 200, 0, "B&C", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "b", "~chunk", 200, 1, "B&C", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "c", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "c", "~chunk", 100, 1, "B&C", "");
	}

	public static void addData(List<Map.Entry<Key, Value>> data, String row, String cf, String cq, String vis, String value) {
		data.add(new KeyValue(new Key(new Text(row), new Text(cf), new Text(cq), new Text(vis)), value.getBytes()));
	}

	public static void addData(List<Map.Entry<Key, Value>> data, String row, String cf, int chunkSize, int chunkCount, String vis, String value) {
		Text chunkCQ = new Text(FileDataIngest.intToBytes(chunkSize));
		chunkCQ.append(FileDataIngest.intToBytes(chunkCount), 0, 4);
		data.add(new KeyValue(new Key(new Text(row), new Text(cf), chunkCQ, new Text(vis)), value.getBytes()));
	}

	public void testExceptionOnMultipleSetSourceWithoutClose() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<Map.Entry<Key, Value>>(data.iterator());
		pi = new PeekingIterator<Map.Entry<Key, Value>>(data.iterator());
		cis.setSource(pi);
		try {
			cis.setSource(pi);
			TestCase.assertNotNull(null);
		} catch (IOException e) {
			TestCase.assertNull(null);
		}
		cis.close();
	}

	public void testExceptionOnGetVisBeforeClose() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<Map.Entry<Key, Value>>(data.iterator());
		cis.setSource(pi);
		try {
			cis.getVisibilities();
			TestCase.assertNotNull(null);
		} catch (RuntimeException e) {
			TestCase.assertNull(null);
		}
		cis.close();
		cis.getVisibilities();
	}

	public void testReadIntoBufferSmallerThanChunks() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[5];
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<Map.Entry<Key, Value>>(data.iterator());
		cis.setSource(pi);
		int read;
		TestCase.assertEquals((read = cis.read(b)), 5);
		TestCase.assertEquals(new String(b, 0, read), "asdfj");
		TestCase.assertEquals((read = cis.read(b)), 3);
		TestCase.assertEquals(new String(b, 0, read), "kl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 5);
		TestCase.assertEquals(new String(b, 0, read), "qwert");
		TestCase.assertEquals((read = cis.read(b)), 5);
		TestCase.assertEquals(new String(b, 0, read), "yuiop");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B, B&C, D]");
		cis.close();
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 5);
		TestCase.assertEquals(new String(b, 0, read), "asdfj");
		TestCase.assertEquals((read = cis.read(b)), 5);
		TestCase.assertEquals(new String(b, 0, read), "kl;as");
		TestCase.assertEquals((read = cis.read(b)), 5);
		TestCase.assertEquals(new String(b, 0, read), "dfjkl");
		TestCase.assertEquals((read = cis.read(b)), 1);
		TestCase.assertEquals(new String(b, 0, read), ";");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.close();
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 5);
		TestCase.assertEquals(new String(b, 0, read), "asdfj");
		TestCase.assertEquals((read = cis.read(b)), 3);
		TestCase.assertEquals(new String(b, 0, read), "kl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		TestCase.assertFalse(pi.hasNext());
	}

	public void testReadIntoBufferLargerThanChunks() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<Map.Entry<Key, Value>>(data.iterator());
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 8);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 10);
		TestCase.assertEquals(new String(b, 0, read), "qwertyuiop");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B, B&C, D]");
		cis.close();
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 16);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.close();
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 8);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		TestCase.assertFalse(pi.hasNext());
	}

	public void testWithAccumulo() throws IOException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		Connector conn = new MockInstance().getConnector("root", new PasswordToken(""));
		conn.tableOperations().create("test");
		BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());
		for (Map.Entry<Key, Value> e : data) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		Scanner scan = conn.createScanner("test", new Authorizations("A", "B", "C", "D"));
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<Map.Entry<Key, Value>>(scan.iterator());
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 8);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 10);
		TestCase.assertEquals(new String(b, 0, read), "qwertyuiop");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B, B&C, D]");
		cis.close();
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 16);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.close();
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 8);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		TestCase.assertFalse(pi.hasNext());
	}

	private static void assumeExceptionOnRead(ChunkInputStream cis, byte[] b) {
		try {
			cis.read(b);
			TestCase.assertNotNull(null);
		} catch (IOException e) {
			ChunkInputStreamTest.log.debug(("EXCEPTION " + (e.getMessage())));
			TestCase.assertNull(null);
		}
	}

	private static void assumeExceptionOnClose(ChunkInputStream cis) {
		try {
			cis.close();
			TestCase.assertNotNull(null);
		} catch (IOException e) {
			ChunkInputStreamTest.log.debug(("EXCEPTION " + (e.getMessage())));
			TestCase.assertNull(null);
		}
	}

	public void testBadData() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<Map.Entry<Key, Value>>(baddata.iterator());
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
		TestCase.assertEquals(cis.getVisibilities().toString(), "[B, C]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
		TestCase.assertEquals(cis.getVisibilities().toString(), "[D, E]");
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 8);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[F, G]");
		cis.close();
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		cis.close();
		TestCase.assertEquals(cis.getVisibilities().toString(), "[I, J]");
		try {
			cis.setSource(pi);
			TestCase.assertNotNull(null);
		} catch (IOException e) {
			TestCase.assertNull(null);
		}
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
		TestCase.assertEquals(cis.getVisibilities().toString(), "[K]");
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[L]");
		cis.close();
		TestCase.assertFalse(pi.hasNext());
		pi = new PeekingIterator<Map.Entry<Key, Value>>(baddata.iterator());
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
	}

	public void testBadDataWithoutClosing() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<Map.Entry<Key, Value>>(baddata.iterator());
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		TestCase.assertEquals(cis.getVisibilities().toString(), "[B, C]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		TestCase.assertEquals(cis.getVisibilities().toString(), "[D, E]");
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 8);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[F, G]");
		cis.close();
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		TestCase.assertEquals(cis.getVisibilities().toString(), "[I, J]");
		try {
			cis.setSource(pi);
			TestCase.assertNotNull(null);
		} catch (IOException e) {
			TestCase.assertNull(null);
		}
		TestCase.assertEquals(cis.getVisibilities().toString(), "[K]");
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), (-1));
		TestCase.assertEquals(cis.getVisibilities().toString(), "[L]");
		cis.close();
		TestCase.assertFalse(pi.hasNext());
		pi = new PeekingIterator<Map.Entry<Key, Value>>(baddata.iterator());
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
	}

	public void testMultipleChunkSizes() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<Map.Entry<Key, Value>>(multidata.iterator());
		b = new byte[20];
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 8);
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.setSource(pi);
		TestCase.assertEquals((read = cis.read(b)), 8);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B, B&C]");
		TestCase.assertFalse(pi.hasNext());
	}

	public void testSingleByteRead() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<Map.Entry<Key, Value>>(data.iterator());
		cis.setSource(pi);
		TestCase.assertEquals(((byte) ('a')), ((byte) (cis.read())));
		TestCase.assertEquals(((byte) ('s')), ((byte) (cis.read())));
		TestCase.assertEquals(((byte) ('d')), ((byte) (cis.read())));
		TestCase.assertEquals(((byte) ('f')), ((byte) (cis.read())));
		TestCase.assertEquals(((byte) ('j')), ((byte) (cis.read())));
		TestCase.assertEquals(((byte) ('k')), ((byte) (cis.read())));
		TestCase.assertEquals(((byte) ('l')), ((byte) (cis.read())));
		TestCase.assertEquals(((byte) (';')), ((byte) (cis.read())));
		TestCase.assertEquals(cis.read(), (-1));
		cis.close();
		TestCase.assertEquals(cis.getVisibilities().toString(), "[A&B]");
	}
}

