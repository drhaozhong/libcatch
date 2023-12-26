package org.apache.accumulo.examples.simple.filedata;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import junit.framework.TestCase;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class ChunkInputFormatTest extends TestCase {
	private static final Logger log = Logger.getLogger(ChunkInputStream.class);

	List<Map.Entry<Key, Value>> data;

	List<Map.Entry<Key, Value>> baddata;

	{
		data = new ArrayList<Map.Entry<Key, Value>>();
		ChunkInputStreamTest.addData(data, "a", "refs", "ida\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(data, "a", "refs", "ida\u0000name", "A&B", "name");
		ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(data, "b", "refs", "ida\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(data, "b", "refs", "ida\u0000name", "A&B", "name");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "B&C", "");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "D", "");
		baddata = new ArrayList<Map.Entry<Key, Value>>();
		ChunkInputStreamTest.addData(baddata, "c", "refs", "ida\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(baddata, "c", "refs", "ida\u0000name", "A&B", "name");
	}

	public static void entryEquals(Map.Entry<Key, Value> e1, Map.Entry<Key, Value> e2) {
		TestCase.assertEquals(e1.getKey(), e2.getKey());
		TestCase.assertEquals(e1.getValue(), e2.getValue());
	}

	public void test() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		MockInstance instance = new MockInstance("instance1");
		Connector conn = instance.getConnector("root", "".getBytes());
		conn.tableOperations().create("test");
		BatchWriter bw = conn.createBatchWriter("test", 100000L, 100L, 5);
		for (Map.Entry<Key, Value> e : data) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		JobContext job = new JobContext(new Configuration(), new JobID());
		ChunkInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "test", new Authorizations("A", "B", "C", "D"));
		ChunkInputFormat.setMockInstance(job.getConfiguration(), "instance1");
		ChunkInputFormat cif = new ChunkInputFormat();
		RangeInputSplit ris = new RangeInputSplit();
		TaskAttemptContext tac = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
		RecordReader<List<Map.Entry<Key, Value>>, InputStream> rr = cif.createRecordReader(ris, tac);
		rr.initialize(ris, tac);
		TestCase.assertTrue(rr.nextKeyValue());
		List<Map.Entry<Key, Value>> info = rr.getCurrentKey();
		InputStream cis = rr.getCurrentValue();
		byte[] b = new byte[20];
		int read;
		TestCase.assertEquals(info.size(), 2);
		ChunkInputFormatTest.entryEquals(info.get(0), data.get(0));
		ChunkInputFormatTest.entryEquals(info.get(1), data.get(1));
		TestCase.assertEquals((read = cis.read(b)), 8);
		TestCase.assertEquals(new String(b, 0, read), "asdfjkl;");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		TestCase.assertTrue(rr.nextKeyValue());
		info = rr.getCurrentKey();
		cis = rr.getCurrentValue();
		TestCase.assertEquals(info.size(), 2);
		ChunkInputFormatTest.entryEquals(info.get(0), data.get(4));
		ChunkInputFormatTest.entryEquals(info.get(1), data.get(5));
		TestCase.assertEquals((read = cis.read(b)), 10);
		TestCase.assertEquals(new String(b, 0, read), "qwertyuiop");
		TestCase.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		TestCase.assertFalse(rr.nextKeyValue());
	}

	public void testErrorOnNextWithoutClose() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		MockInstance instance = new MockInstance("instance2");
		Connector conn = instance.getConnector("root", "".getBytes());
		conn.tableOperations().create("test");
		BatchWriter bw = conn.createBatchWriter("test", 100000L, 100L, 5);
		for (Map.Entry<Key, Value> e : data) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		JobContext job = new JobContext(new Configuration(), new JobID());
		ChunkInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "test", new Authorizations("A", "B", "C", "D"));
		ChunkInputFormat.setMockInstance(job.getConfiguration(), "instance2");
		ChunkInputFormat cif = new ChunkInputFormat();
		RangeInputSplit ris = new RangeInputSplit();
		TaskAttemptContext tac = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
		RecordReader<List<Map.Entry<Key, Value>>, InputStream> crr = cif.createRecordReader(ris, tac);
		crr.initialize(ris, tac);
		TestCase.assertTrue(crr.nextKeyValue());
		InputStream cis = crr.getCurrentValue();
		byte[] b = new byte[5];
		int read;
		TestCase.assertEquals((read = cis.read(b)), 5);
		TestCase.assertEquals(new String(b, 0, read), "asdfj");
		try {
			crr.nextKeyValue();
			TestCase.assertNotNull(null);
		} catch (Exception e) {
			ChunkInputFormatTest.log.debug(("EXCEPTION " + (e.getMessage())));
			TestCase.assertNull(null);
		}
	}

	public void testInfoWithoutChunks() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		MockInstance instance = new MockInstance("instance3");
		Connector conn = instance.getConnector("root", "".getBytes());
		conn.tableOperations().create("test");
		BatchWriter bw = conn.createBatchWriter("test", 100000L, 100L, 5);
		for (Map.Entry<Key, Value> e : baddata) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		JobContext job = new JobContext(new Configuration(), new JobID());
		ChunkInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "test", new Authorizations("A", "B", "C", "D"));
		ChunkInputFormat.setMockInstance(job.getConfiguration(), "instance3");
		ChunkInputFormat cif = new ChunkInputFormat();
		RangeInputSplit ris = new RangeInputSplit();
		TaskAttemptContext tac = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
		RecordReader<List<Map.Entry<Key, Value>>, InputStream> crr = cif.createRecordReader(ris, tac);
		crr.initialize(ris, tac);
		TestCase.assertTrue(crr.nextKeyValue());
		List<Map.Entry<Key, Value>> info = crr.getCurrentKey();
		InputStream cis = crr.getCurrentValue();
		byte[] b = new byte[20];
		TestCase.assertEquals(info.size(), 2);
		ChunkInputFormatTest.entryEquals(info.get(0), baddata.get(0));
		ChunkInputFormatTest.entryEquals(info.get(1), baddata.get(1));
		try {
			cis.read(b);
			TestCase.assertNotNull(null);
		} catch (Exception e) {
			ChunkInputFormatTest.log.debug(("EXCEPTION " + (e.getMessage())));
			TestCase.assertNull(null);
		}
		try {
			cis.close();
			TestCase.assertNotNull(null);
		} catch (Exception e) {
			ChunkInputFormatTest.log.debug(("EXCEPTION " + (e.getMessage())));
			TestCase.assertNull(null);
		}
	}
}

