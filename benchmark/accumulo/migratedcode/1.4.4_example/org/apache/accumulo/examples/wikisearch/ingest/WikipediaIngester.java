package org.apache.accumulo.examples.wikisearch.ingest;


import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.examples.wikisearch.iterator.GlobalIndexUidCombiner;
import org.apache.accumulo.examples.wikisearch.iterator.TextIndexCombiner;
import org.apache.accumulo.examples.wikisearch.reader.AggregatingRecordReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.accumulo.core.iterators.LongCombiner.Type.VARLEN;


public class WikipediaIngester extends Configured implements Tool {
	public static final String INGEST_LANGUAGE = "wikipedia.ingest_language";

	public static final String SPLIT_FILE = "wikipedia.split_file";

	public static final String TABLE_NAME = "wikipedia.table";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaIngester(), args);
		System.exit(res);
	}

	private void createTables(TableOperations tops, String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		String indexTableName = tableName + "Index";
		String reverseIndexTableName = tableName + "ReverseIndex";
		String metadataTableName = tableName + "Metadata";
		if (!(tops.exists(tableName))) {
			String textIndexFamilies = WikipediaMapper.TOKENS_FIELD_NAME;
			tops.create(tableName);
			if ((textIndexFamilies.length()) > 0) {
				System.out.println(("Adding content combiner on the fields: " + textIndexFamilies));
				IteratorSetting setting = new IteratorSetting(10, TextIndexCombiner.class);
				List<IteratorSetting.Column> columns = new ArrayList<IteratorSetting.Column>();
				for (String family : StringUtils.split(textIndexFamilies, ',')) {
					columns.add(new IteratorSetting.Column(("fi\u0000" + family)));
				}
				TextIndexCombiner.setColumns(setting, columns);
				TextIndexCombiner.setLossyness(setting, true);
				tops.attachIterator(tableName, setting, EnumSet.allOf(IteratorUtil.IteratorScope.class));
			}
			tops.setLocalityGroups(tableName, Collections.singletonMap("WikipediaDocuments", Collections.singleton(new Text(WikipediaMapper.DOCUMENT_COLUMN_FAMILY))));
		}
		if (!(tops.exists(indexTableName))) {
			tops.create(indexTableName);
			IteratorSetting setting = new IteratorSetting(19, "UIDAggregator", GlobalIndexUidCombiner.class);
			GlobalIndexUidCombiner.setCombineAllColumns(setting, true);
			GlobalIndexUidCombiner.setLossyness(setting, true);
			tops.attachIterator(indexTableName, setting, EnumSet.allOf(IteratorUtil.IteratorScope.class));
		}
		if (!(tops.exists(reverseIndexTableName))) {
			tops.create(reverseIndexTableName);
			IteratorSetting setting = new IteratorSetting(19, "UIDAggregator", GlobalIndexUidCombiner.class);
			GlobalIndexUidCombiner.setCombineAllColumns(setting, true);
			GlobalIndexUidCombiner.setLossyness(setting, true);
			tops.attachIterator(reverseIndexTableName, setting, EnumSet.allOf(IteratorUtil.IteratorScope.class));
		}
		if (!(tops.exists(metadataTableName))) {
			tops.create(metadataTableName);
			IteratorSetting setting = new IteratorSetting(10, SummingCombiner.class);
			SummingCombiner.setColumns(setting, Collections.singletonList(new IteratorSetting.Column("f")));
			SummingCombiner.setEncodingType(setting, VARLEN);
			tops.attachIterator(metadataTableName, setting, EnumSet.allOf(IteratorUtil.IteratorScope.class));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Ingest Wikipedia");
		Configuration conf = job.getConfiguration();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		String tablename = WikipediaConfiguration.getTableName(conf);
		String zookeepers = WikipediaConfiguration.getZookeepers(conf);
		String instanceName = WikipediaConfiguration.getInstanceName(conf);
		String user = WikipediaConfiguration.getUser(conf);
		byte[] password = WikipediaConfiguration.getPassword(conf);
		Connector connector = WikipediaConfiguration.getConnector(conf);
		TableOperations tops = connector.tableOperations();
		createTables(tops, tablename);
		configureJob(job);
		List<Path> inputPaths = new ArrayList<Path>();
		SortedSet<String> languages = new TreeSet<String>();
		FileSystem fs = FileSystem.get(conf);
		Path parent = new Path(conf.get("wikipedia.input"));
		listFiles(parent, fs, inputPaths, languages);
		System.out.println(((("Input files in " + parent) + ":") + (inputPaths.size())));
		Path[] inputPathsArray = new Path[inputPaths.size()];
		inputPaths.toArray(inputPathsArray);
		System.out.println(("Languages:" + (languages.size())));
		FileInputFormat.setInputPaths(job, inputPathsArray);
		job.setMapperClass(WikipediaMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), user, password, true, tablename);
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), instanceName, zookeepers);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static final PathFilter partFilter = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return path.getName().startsWith("part");
		}
	};

	protected void configureJob(Job job) {
		Configuration conf = job.getConfiguration();
		job.setJarByClass(WikipediaIngester.class);
		job.setInputFormatClass(WikipediaInputFormat.class);
		conf.set(AggregatingRecordReader.START_TOKEN, "<page>");
		conf.set(AggregatingRecordReader.END_TOKEN, "</page>");
	}

	protected static final Pattern filePattern = Pattern.compile("([a-z_]+).*.xml(.bz2)?");

	protected void listFiles(Path path, FileSystem fs, List<Path> files, Set<String> languages) throws IOException {
		for (FileStatus status : fs.listStatus(path)) {
			if (status.isDir()) {
				listFiles(status.getPath(), fs, files, languages);
			}else {
				Path p = status.getPath();
				Matcher matcher = WikipediaIngester.filePattern.matcher(p.getName());
				if (matcher.matches()) {
					languages.add(matcher.group(1));
					files.add(p);
				}
			}
		}
	}
}

