package org.apache.accumulo.test.examples.simple.filedata;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.simple.filedata.ChunkInputFormat;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;


public class ChunkInputFormatIT extends AccumuloClusterHarness {
	private static Multimap<String, AssertionError> assertionErrors = ArrayListMultimap.create();

	private static final Authorizations AUTHS = new Authorizations("A", "B", "C", "D");

	private static List<Map.Entry<Key, Value>> data;

	private static List<Map.Entry<Key, Value>> baddata;

	private Connector conn;

	private String tableName;

	@org.junit.Before
	public void setupInstance() throws Exception {
		conn = getConnector();
		tableName = getUniqueNames(1)[0];
		conn.securityOperations().changeUserAuthorizations(conn.whoami(), ChunkInputFormatIT.AUTHS);
	}

	@org.junit.BeforeClass
	public static void setupClass() {
		System.setProperty("hadoop.tmp.dir", ((System.getProperty("user.dir")) + "/target/hadoop-tmp"));
		ChunkInputFormatIT.data = new ArrayList<>();
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "a", "refs", "ida\u0000ext", "A&B", "ext");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "a", "refs", "ida\u0000name", "A&B", "name");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "a", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "b", "refs", "ida\u0000ext", "A&B", "ext");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "b", "refs", "ida\u0000name", "A&B", "name");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "b", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "b", "~chunk", 100, 1, "B&C", "");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.data, "b", "~chunk", 100, 1, "D", "");
		ChunkInputFormatIT.baddata = new ArrayList<>();
		ChunkInputStreamIT.addData(ChunkInputFormatIT.baddata, "c", "refs", "ida\u0000ext", "A&B", "ext");
		ChunkInputStreamIT.addData(ChunkInputFormatIT.baddata, "c", "refs", "ida\u0000name", "A&B", "name");
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
				String table = context.getConfiguration().get("MRTester_tableName");
				Assert.assertNotNull(table);
				byte[] b = new byte[20];
				int read;
				try {
					switch (count) {
						case 0 :
							Assert.assertEquals(key.size(), 2);
							ChunkInputFormatIT.entryEquals(key.get(0), ChunkInputFormatIT.data.get(0));
							ChunkInputFormatIT.entryEquals(key.get(1), ChunkInputFormatIT.data.get(1));
							Assert.assertEquals((read = value.read(b)), 8);
							Assert.assertEquals(new String(b, 0, read), "asdfjkl;");
							Assert.assertEquals((read = value.read(b)), (-1));
							break;
						case 1 :
							Assert.assertEquals(key.size(), 2);
							ChunkInputFormatIT.entryEquals(key.get(0), ChunkInputFormatIT.data.get(4));
							ChunkInputFormatIT.entryEquals(key.get(1), ChunkInputFormatIT.data.get(5));
							Assert.assertEquals((read = value.read(b)), 10);
							Assert.assertEquals(new String(b, 0, read), "qwertyuiop");
							Assert.assertEquals((read = value.read(b)), (-1));
							break;
						default :
							Assert.fail();
					}
				} catch (AssertionError e) {
					ChunkInputFormatIT.assertionErrors.put(table, e);
				} finally {
					value.close();
				}
				(count)++;
			}

			@Override
			protected void cleanup(Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream>.Context context) throws IOException, InterruptedException {
				String table = context.getConfiguration().get("MRTester_tableName");
				Assert.assertNotNull(table);
				try {
					Assert.assertEquals(2, count);
				} catch (AssertionError e) {
					ChunkInputFormatIT.assertionErrors.put(table, e);
				}
			}
		}

		public static class TestNoClose extends Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream> {
			int count = 0;

			@Override
			protected void map(List<Map.Entry<Key, Value>> key, InputStream value, Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream>.Context context) throws IOException, InterruptedException {
				String table = context.getConfiguration().get("MRTester_tableName");
				Assert.assertNotNull(table);
				byte[] b = new byte[5];
				int read;
				try {
					switch (count) {
						case 0 :
							Assert.assertEquals((read = value.read(b)), 5);
							Assert.assertEquals(new String(b, 0, read), "asdfj");
							break;
						default :
							Assert.fail();
					}
				} catch (AssertionError e) {
					ChunkInputFormatIT.assertionErrors.put(table, e);
				}
				(count)++;
				try {
					context.nextKeyValue();
					Assert.fail();
				} catch (IOException ioe) {
					ChunkInputFormatIT.assertionErrors.put((table + "_map_ioexception"), new AssertionError(toString(), ioe));
				}
			}
		}

		public static class TestBadData extends Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream> {
			@Override
			protected void map(List<Map.Entry<Key, Value>> key, InputStream value, Mapper<List<Map.Entry<Key, Value>>, InputStream, List<Map.Entry<Key, Value>>, InputStream>.Context context) throws IOException, InterruptedException {
				String table = context.getConfiguration().get("MRTester_tableName");
				Assert.assertNotNull(table);
				byte[] b = new byte[20];
				try {
					Assert.assertEquals(key.size(), 2);
					ChunkInputFormatIT.entryEquals(key.get(0), ChunkInputFormatIT.baddata.get(0));
					ChunkInputFormatIT.entryEquals(key.get(1), ChunkInputFormatIT.baddata.get(1));
				} catch (AssertionError e) {
					ChunkInputFormatIT.assertionErrors.put(table, e);
				}
				try {
					Assert.assertFalse(((value.read(b)) > 0));
					try {
						Assert.fail();
					} catch (AssertionError e) {
						ChunkInputFormatIT.assertionErrors.put(table, e);
					}
				} catch (Exception e) {
				}
				try {
					value.close();
					try {
						Assert.fail();
					} catch (AssertionError e) {
						ChunkInputFormatIT.assertionErrors.put(table, e);
					}
				} catch (Exception e) {
				}
			}
		}

		@Override
		public int run(String[] args) throws Exception {
			if ((args.length) != 2) {
				throw new IllegalArgumentException((("Usage : " + (ChunkInputFormatIT.CIFTester.class.getName())) + " <table> <mapperClass>"));
			}
			String table = args[0];
			ChunkInputFormatIT.assertionErrors.put(table, new AssertionError("Dummy"));
			ChunkInputFormatIT.assertionErrors.put((table + "_map_ioexception"), new AssertionError("Dummy_ioexception"));
			getConf().set("MRTester_tableName", table);
			Job job = getInstance(getConf());
			job.setJobName((((this.getClass().getSimpleName()) + "_") + (System.currentTimeMillis())));
			job.setJarByClass(this.getClass());
			job.setInputFormatClass(ChunkInputFormat.class);
			ChunkInputFormat.setZooKeeperInstance(job, AccumuloClusterHarness.getCluster().getClientConfig());
			ChunkInputFormat.setConnectorInfo(job, AccumuloClusterHarness.getAdminPrincipal(), AccumuloClusterHarness.getAdminToken());
			ChunkInputFormat.setInputTableName(job, table);
			ChunkInputFormat.setScanAuthorizations(job, ChunkInputFormatIT.AUTHS);
			@SuppressWarnings("unchecked")
			Class<? extends Mapper<?, ?, ?, ?>> forName = ((Class<? extends Mapper<?, ?, ?, ?>>) (Class.forName(args[1])));
			job.setMapperClass(forName);
			job.setMapOutputKeyClass(Key.class);
			job.setMapOutputValueClass(Value.class);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
			return job.isSuccessful() ? 0 : 1;
		}

		public static int main(String... args) throws Exception {
			Configuration conf = new Configuration();
			conf.set("mapreduce.framework.name", "local");
			conf.set("mapreduce.cluster.local.dir", new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
			return ToolRunner.run(conf, new ChunkInputFormatIT.CIFTester(), args);
		}
	}

	@org.junit.Test
	public void test() throws Exception {
		conn.tableOperations().create(tableName);
		BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
		for (Map.Entry<Key, Value> e : ChunkInputFormatIT.data) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		Assert.assertEquals(0, ChunkInputFormatIT.CIFTester.main(tableName, ChunkInputFormatIT.CIFTester.TestMapper.class.getName()));
		Assert.assertEquals(1, ChunkInputFormatIT.assertionErrors.get(tableName).size());
	}

	@org.junit.Test
	public void testErrorOnNextWithoutClose() throws Exception {
		conn.tableOperations().create(tableName);
		BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
		for (Map.Entry<Key, Value> e : ChunkInputFormatIT.data) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		Assert.assertEquals(1, ChunkInputFormatIT.CIFTester.main(tableName, ChunkInputFormatIT.CIFTester.TestNoClose.class.getName()));
		Assert.assertEquals(1, ChunkInputFormatIT.assertionErrors.get(tableName).size());
		Assert.assertEquals(2, ChunkInputFormatIT.assertionErrors.get(((tableName) + "_map_ioexception")).size());
	}

	@org.junit.Test
	public void testInfoWithoutChunks() throws Exception {
		conn.tableOperations().create(tableName);
		BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
		for (Map.Entry<Key, Value> e : ChunkInputFormatIT.baddata) {
			Key k = e.getKey();
			Mutation m = new Mutation(k.getRow());
			m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
			bw.addMutation(m);
		}
		bw.close();
		Assert.assertEquals(0, ChunkInputFormatIT.CIFTester.main(tableName, ChunkInputFormatIT.CIFTester.TestBadData.class.getName()));
		Assert.assertEquals(1, ChunkInputFormatIT.assertionErrors.get(tableName).size());
	}
}

