package org.apache.accumulo.examples.simple.filedata;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.simple.mapreduce.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;


public class ChunkInputFormatTest {
	private static AssertionError e0 = null;

	private static AssertionError e1 = null;

	private static AssertionError e2 = null;

	private static IOException e3 = null;

	private static final Authorizations AUTHS = new Authorizations("A", "B", "C", "D");

	private static List<Map.Entry<Key, Value>> data;

	private static List<Map.Entry<Key, Value>> baddata;

	@org.junit.BeforeClass
	public static void setupClass() {
		System.setProperty("hadoop.tmp.dir", ((System.getProperty("user.dir")) + "/target/hadoop-tmp"));
		ChunkInputFormatTest.data = new ArrayList<Map.Entry<Key, Value>>();
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "a", "refs", "ida\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "a", "refs", "ida\u0000name", "A&B", "name");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "a", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "b", "refs", "ida\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "b", "refs", "ida\u0000name", "A&B", "name");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "b", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "b", "~chunk", 100, 1, "B&C", "");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.data, "b", "~chunk", 100, 1, "D", "");
		ChunkInputFormatTest.baddata = new ArrayList<Map.Entry<Key, Value>>();
		ChunkInputStreamTest.addData(ChunkInputFormatTest.baddata, "c", "refs", "ida\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(ChunkInputFormatTest.baddata, "c", "refs", "ida\u0000name", "A&B", "name");
	}

	public static void entryEquals(Map.Entry<Key, Value> e1, Map.Entry<Key, Value> e2) {
		Assert.assertEquals(e1.getKey(), e2.getKey());
		Assert.assertEquals(e1.getValue(), e2.getValue());
	}

	public static class CIFTester extends Configured implements Tool {
		public static class TestMapper extends Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream> {
			int count = 0;

			@Override
			protected void map(List<Map.Entry<Key, Value>> key, InputStream value, Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream>.Context context) throws IOException, InterruptedException {
				byte[] b = new byte[20];
				int read;
				try {
					switch (count) {
						case 0 :
							Assert.assertEquals(key.size(), 2);
							ChunkInputFormatTest.entryEquals(key.get(0), ChunkInputFormatTest.data.get(0));
							ChunkInputFormatTest.entryEquals(key.get(1), ChunkInputFormatTest.data.get(1));
							Assert.assertEquals((read = value.read(b)), 8);
							Assert.assertEquals(new String(b, 0, read), "asdfjkl;");
							Assert.assertEquals((read = value.read(b)), (-1));
							break;
						case 1 :
							Assert.assertEquals(key.size(), 2);
							ChunkInputFormatTest.entryEquals(key.get(0), ChunkInputFormatTest.data.get(4));
							ChunkInputFormatTest.entryEquals(key.get(1), ChunkInputFormatTest.data.get(5));
							Assert.assertEquals((read = value.read(b)), 10);
							Assert.assertEquals(new String(b, 0, read), "qwertyuiop");
							Assert.assertEquals((read = value.read(b)), (-1));
							break;
						default :
							Assert.assertTrue(false);
					}
				} catch (AssertionError e) {
					ChunkInputFormatTest.e1 = e;
				} finally {
					value.close();
				}
				(count)++;
			}

			@Override
			protected void cleanup(Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream>.Context context) throws IOException, InterruptedException {
				try {
					Assert.assertEquals(2, count);
				} catch (AssertionError e) {
					ChunkInputFormatTest.e2 = e;
				}
			}
		}

		public static class TestNoClose extends Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream> {
			int count = 0;

			@Override
			protected void map(List<Map.Entry<Key, Value>> key, InputStream value, Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream>.Context context) throws IOException, InterruptedException {
				byte[] b = new byte[5];
				int read;
				try {
					switch (count) {
						case 0 :
							Assert.assertEquals((read = value.read(b)), 5);
							Assert.assertEquals(new String(b, 0, read), "asdfj");
							break;
						default :
							Assert.assertTrue(false);
					}
				} catch (AssertionError e) {
					ChunkInputFormatTest.e1 = e;
				}
				(count)++;
				try {
					context.nextKeyValue();
					Assert.assertTrue(false);
				} catch (IOException ioe) {
					ChunkInputFormatTest.e3 = ioe;
				}
			}
		}

		public static class TestBadData extends Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream> {
			@Override
			protected void map(List<Map.Entry<Key, Value>> key, InputStream value, Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream>.Context context) throws IOException, InterruptedException {
				byte[] b = new byte[20];
				try {
					Assert.assertEquals(key.size(), 2);
					ChunkInputFormatTest.entryEquals(key.get(0), ChunkInputFormatTest.baddata.get(0));
					ChunkInputFormatTest.entryEquals(key.get(1), ChunkInputFormatTest.baddata.get(1));
				} catch (AssertionError e) {
					ChunkInputFormatTest.e0 = e;
				}
				try {
					value.read(b);
					try {
						Assert.assertTrue(false);
					} catch (AssertionError e) {
						ChunkInputFormatTest.e1 = e;
					}
				} catch (Exception e) {
				}
				try {
					value.close();
					try {
						Assert.assertTrue(false);
					} catch (AssertionError e) {
						ChunkInputFormatTest.e2 = e;
					}
				} catch (Exception e) {
				}
			}
		}

		@Override
		public int run(String[] args) throws Exception {
			if ((args.length) != 5) {
				throw new IllegalArgumentException((("Usage : " + (ChunkInputFormatTest.CIFTester.class.getName())) + " <instance name> <user> <pass> <table> <mapperClass>"));
			}
			String instance = args[0];
			String user = args[1];
			String pass = args[2];
			String table = args[3];
			Job job = JobUtil.getJob(getConf());
			job.setJobName((((this.getClass().getSimpleName()) + "_") + (System.currentTimeMillis())));
			job.setJarByClass(this.getClass());
			job.setInputFormatClass(ChunkInputFormat.class);
			ChunkInputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
			ChunkInputFormat.setInputTableName(job, table);
			ChunkInputFormat.setScanAuthorizations(job, ChunkInputFormatTest.AUTHS);
			ChunkInputFormat.setMockInstance(job, instance);
			@SuppressWarnings("unchecked")
			Class<? extends Mapper<?, ?, ?, ?>> forName = ((Class<? extends Mapper<?, ?, ?, ?>>) (Class.forName(args[4])));
			job.setMapperClass(forName);
			job.setMapOutputKeyClass(Key.class);
			job.setMapOutputValueClass(Value.class);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
			return job.isSuccessful() ? 0 : 1;
		}

		public static int main(String... args) throws Exception {
			return ToolRunner.run(new Configuration(), new ChunkInputFormatTest.CIFTester(), args);
		}
	}

	@org.junit.Test
	public void test() throws Exception {
		MockInstance instance = new MockInstance("instance1");
		Connector conn = instance.getConnector("root", new PasswordToken(""));
		conn.tableOperations().create("test");
		BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());
		for (Map.Entry<Key, Value> e : ChunkInputFormatTest.data) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		Assert.assertEquals(0, ChunkInputFormatTest.CIFTester.main("instance1", "root", "", "test", ChunkInputFormatTest.CIFTester.TestMapper.class.getName()));
		Assert.assertNull(ChunkInputFormatTest.e1);
		Assert.assertNull(ChunkInputFormatTest.e2);
	}

	@org.junit.Test
	public void testErrorOnNextWithoutClose() throws Exception {
		MockInstance instance = new MockInstance("instance2");
		Connector conn = instance.getConnector("root", new PasswordToken(""));
		conn.tableOperations().create("test");
		BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());
		for (Map.Entry<Key, Value> e : ChunkInputFormatTest.data) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		Assert.assertEquals(1, ChunkInputFormatTest.CIFTester.main("instance2", "root", "", "test", ChunkInputFormatTest.CIFTester.TestNoClose.class.getName()));
		Assert.assertNull(ChunkInputFormatTest.e1);
		Assert.assertNull(ChunkInputFormatTest.e2);
		Assert.assertNotNull(ChunkInputFormatTest.e3);
	}

	@org.junit.Test
	public void testInfoWithoutChunks() throws Exception {
		MockInstance instance = new MockInstance("instance3");
		Connector conn = instance.getConnector("root", new PasswordToken(""));
		conn.tableOperations().create("test");
		BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());
		for (Map.Entry<Key, Value> e : ChunkInputFormatTest.baddata) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		Assert.assertEquals(0, ChunkInputFormatTest.CIFTester.main("instance3", "root", "", "test", ChunkInputFormatTest.CIFTester.TestBadData.class.getName()));
		Assert.assertNull(ChunkInputFormatTest.e0);
		Assert.assertNull(ChunkInputFormatTest.e1);
		Assert.assertNull(ChunkInputFormatTest.e2);
	}
}

