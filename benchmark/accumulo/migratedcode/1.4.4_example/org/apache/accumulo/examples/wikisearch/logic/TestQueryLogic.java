package org.apache.accumulo.examples.wikisearch.logic;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaConfiguration;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaInputFormat;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaMapper;
import org.apache.accumulo.examples.wikisearch.parser.RangeCalculator;
import org.apache.accumulo.examples.wikisearch.reader.AggregatingRecordReader;
import org.apache.accumulo.examples.wikisearch.sample.Document;
import org.apache.accumulo.examples.wikisearch.sample.Field;
import org.apache.accumulo.examples.wikisearch.sample.Results;
import org.apache.accumulo.server.tabletserver.TabletStatsKeeper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static org.apache.hadoop.mapreduce.Mapper.Context.<init>;


public class TestQueryLogic {
	private static final String METADATA_TABLE_NAME = "wikiMetadata";

	private static final String TABLE_NAME = "wiki";

	private static final String INDEX_TABLE_NAME = "wikiIndex";

	private static final String RINDEX_TABLE_NAME = "wikiReverseIndex";

	private static final String[] TABLE_NAMES = new String[]{ TestQueryLogic.METADATA_TABLE_NAME, TestQueryLogic.TABLE_NAME, TestQueryLogic.RINDEX_TABLE_NAME, TestQueryLogic.INDEX_TABLE_NAME };

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

	private QueryLogic table = null;

	@org.junit.Before
	public void setup() throws Exception {
		Logger.getLogger(AbstractQueryLogic.class).setLevel(Level.DEBUG);
		Logger.getLogger(QueryLogic.class).setLevel(Level.DEBUG);
		Logger.getLogger(RangeCalculator.class).setLevel(Level.DEBUG);
		conf.set(AggregatingRecordReader.START_TOKEN, "<page>");
		conf.set(AggregatingRecordReader.END_TOKEN, "</page>");
		conf.set(WikipediaConfiguration.TABLE_NAME, TestQueryLogic.TABLE_NAME);
		conf.set(WikipediaConfiguration.NUM_PARTITIONS, "1");
		conf.set(WikipediaConfiguration.NUM_GROUPS, "1");
		MockInstance i = new MockInstance();
		c = i.getConnector("root", "");
		for (String table : TestQueryLogic.TABLE_NAMES) {
			try {
				c.tableOperations().delete(table);
			} catch (Exception ex) {
			}
			c.tableOperations().create(table);
			writerMap.put(new Text(table), c.createBatchWriter(table, 1000L, 1000L, 1));
		}
		TaskAttemptID id = new TaskAttemptID();
		TaskAttemptContext context = new TaskAttemptContext(conf, id);
		RawLocalFileSystem fs = new RawLocalFileSystem();
		fs.setConf(conf);
		URL url = ClassLoader.getSystemResource("enwiki-20110901-001.xml");
		Assert.assertNotNull(url);
		File data = new File(url.toURI());
		Path tmpFile = new Path(data.getAbsolutePath());
		WikipediaInputFormat.WikipediaInputSplit split = new WikipediaInputFormat.WikipediaInputSplit(new FileSplit(tmpFile, 0, fs.pathToFile(tmpFile).length(), null), 0);
		AggregatingRecordReader rr = new AggregatingRecordReader();
		Path ocPath = new Path(tmpFile, "oc");
		OutputCommitter oc = new FileOutputCommitter(ocPath, context);
		fs.deleteOnExit(ocPath);
		TabletStatsKeeper sr = new TabletStatsKeeper();
		rr.initialize(split, context);
		TestQueryLogic.MockAccumuloRecordWriter rw = new TestQueryLogic.MockAccumuloRecordWriter();
		WikipediaMapper mapper = new WikipediaMapper();
		Mapper<LongWritable, Text, Text, Mutation>.Context con = mapper.new Context(conf, id, rr, rw, oc, sr, split);
		mapper.run(con);
		rw.close(context);
		table = new QueryLogic();
		table.setMetadataTableName(TestQueryLogic.METADATA_TABLE_NAME);
		table.setTableName(TestQueryLogic.TABLE_NAME);
		table.setIndexTableName(TestQueryLogic.INDEX_TABLE_NAME);
		table.setReverseIndexTableName(TestQueryLogic.RINDEX_TABLE_NAME);
		table.setUseReadAheadIterator(false);
	}

	void debugQuery(String tableName) throws Exception {
		Scanner s = c.createScanner(tableName, new Authorizations());
		Range r = new Range();
		s.setRange(r);
		for (Map.Entry<Key, Value> entry : s)
			System.out.println((((entry.getKey().toString()) + " ") + (entry.getValue().toString())));

	}

	@org.junit.Test
	public void testTitle() {
		Logger.getLogger(AbstractQueryLogic.class).setLevel(Level.OFF);
		Logger.getLogger(RangeCalculator.class).setLevel(Level.OFF);
		List<String> auths = new ArrayList<String>();
		auths.add("enwiki");
		Results results = table.runQuery(c, auths, "TITLE == 'afghanistanhistory'", null, null, null);
		for (Document doc : results.getResults()) {
			System.out.println(("id: " + (doc.getId())));
			for (Field field : doc.getFields())
				System.out.println((((field.getFieldName()) + " -> ") + (field.getFieldValue())));

		}
	}
}

