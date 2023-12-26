package org.apache.accumulo.examples.wikisearch.ingest;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.wikisearch.reader.AggregatingRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;


public class WikipediaMapperTest {
	private static final String METADATA_TABLE_NAME = "wikiMetadata";

	private static final String TABLE_NAME = "wiki";

	private static final String INDEX_TABLE_NAME = "wikiIndex";

	private static final String RINDEX_TABLE_NAME = "wikiReverseIndex";

	private class MockAccumuloRecordWriter extends RecordWriter<Text, Mutation> {
		@Override
		public void write(Text key, Mutation value) throws IOException, InterruptedException {
			try {
				writerMap.get(key).addMutation(value);
			} catch (MutationsRejectedException e) {
				throw new IOException("Error adding mutation", e);
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				for (BatchWriter w : writerMap.values()) {
					w.flush();
					w.close();
				}
			} catch (MutationsRejectedException e) {
				throw new IOException("Error closing Batch Writer", e);
			}
		}
	}

	private Connector c = null;

	private Configuration conf = new Configuration();

	private HashMap<Text, BatchWriter> writerMap = new HashMap<Text, BatchWriter>();

	@org.junit.Before
	public void setup() throws Exception {
		conf.set(AggregatingRecordReader.START_TOKEN, "<page>");
		conf.set(AggregatingRecordReader.END_TOKEN, "</page>");
		conf.set(WikipediaConfiguration.TABLE_NAME, WikipediaMapperTest.TABLE_NAME);
		conf.set(WikipediaConfiguration.NUM_PARTITIONS, "1");
		conf.set(WikipediaConfiguration.NUM_GROUPS, "1");
		MockInstance i = new MockInstance();
		c = i.getConnector("root", "pass");
		c.tableOperations().delete(WikipediaMapperTest.METADATA_TABLE_NAME);
		c.tableOperations().delete(WikipediaMapperTest.TABLE_NAME);
		c.tableOperations().delete(WikipediaMapperTest.INDEX_TABLE_NAME);
		c.tableOperations().delete(WikipediaMapperTest.RINDEX_TABLE_NAME);
		c.tableOperations().create(WikipediaMapperTest.METADATA_TABLE_NAME);
		c.tableOperations().create(WikipediaMapperTest.TABLE_NAME);
		c.tableOperations().create(WikipediaMapperTest.INDEX_TABLE_NAME);
		c.tableOperations().create(WikipediaMapperTest.RINDEX_TABLE_NAME);
		writerMap.put(new Text(WikipediaMapperTest.METADATA_TABLE_NAME), c.createBatchWriter(WikipediaMapperTest.METADATA_TABLE_NAME, 1000L, 1000L, 1));
		writerMap.put(new Text(WikipediaMapperTest.TABLE_NAME), c.createBatchWriter(WikipediaMapperTest.TABLE_NAME, 1000L, 1000L, 1));
		writerMap.put(new Text(WikipediaMapperTest.INDEX_TABLE_NAME), c.createBatchWriter(WikipediaMapperTest.INDEX_TABLE_NAME, 1000L, 1000L, 1));
		writerMap.put(new Text(WikipediaMapperTest.RINDEX_TABLE_NAME), c.createBatchWriter(WikipediaMapperTest.RINDEX_TABLE_NAME, 1000L, 1000L, 1));
		TaskAttemptID id = new TaskAttemptID();
		TaskAttemptContext context = new TaskAttemptContext(conf, id);
		RawLocalFileSystem fs = new RawLocalFileSystem();
		fs.setConf(conf);
		URL url = ClassLoader.getSystemResource("enwiki-20110901-001.xml");
		Assert.assertNotNull(url);
		File data = new File(url.toURI());
		Path tmpFile = new Path(data.getAbsolutePath());
		InputSplit split = new FileSplit(tmpFile, 0, fs.pathToFile(tmpFile).length(), null);
		AggregatingRecordReader rr = new AggregatingRecordReader();
		Path ocPath = new Path(tmpFile, "oc");
		OutputCommitter oc = new FileOutputCommitter(ocPath, context);
		fs.deleteOnExit(ocPath);
		StandaloneStatusReporter sr = new StandaloneStatusReporter();
		rr.initialize(split, context);
		WikipediaMapperTest.MockAccumuloRecordWriter rw = new WikipediaMapperTest.MockAccumuloRecordWriter();
		WikipediaMapper mapper = new WikipediaMapper();
		Mapper<LongWritable, Text, Text, Mutation>.Context con = mapper.new Context(conf, id, rr, rw, oc, sr, split);
		mapper.run(con);
		rw.close(context);
	}

	private void debugQuery(String tableName) throws Exception {
		Scanner s = c.createScanner(tableName, new Authorizations("all"));
		Range r = new Range();
		s.setRange(r);
		for (Map.Entry<Key, Value> entry : s)
			System.out.println((((entry.getKey().toString()) + " ") + (entry.getValue().toString())));

	}

	public void testViewAllData() throws Exception {
		debugQuery(WikipediaMapperTest.METADATA_TABLE_NAME);
		debugQuery(WikipediaMapperTest.TABLE_NAME);
		debugQuery(WikipediaMapperTest.INDEX_TABLE_NAME);
		debugQuery(WikipediaMapperTest.RINDEX_TABLE_NAME);
	}
}

