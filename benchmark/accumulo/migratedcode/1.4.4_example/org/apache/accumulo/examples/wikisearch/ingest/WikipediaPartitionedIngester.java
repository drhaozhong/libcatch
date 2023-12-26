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
import org.apache.accumulo.examples.wikisearch.output.SortingRFileOutputFormat;
import org.apache.accumulo.examples.wikisearch.reader.AggregatingRecordReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;

import static org.apache.accumulo.core.iterators.LongCombiner.Type.VARLEN;
import static org.apache.hadoop.io.SequenceFile.CompressionType.RECORD;


public class WikipediaPartitionedIngester extends Configured implements Tool {
	private static final Logger log = Logger.getLogger(WikipediaPartitionedIngester.class);

	public static final String INGEST_LANGUAGE = "wikipedia.ingest_language";

	public static final String SPLIT_FILE = "wikipedia.split_file";

	public static final String TABLE_NAME = "wikipedia.table";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPartitionedIngester(), args);
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
		Configuration conf = getConf();
		if (WikipediaConfiguration.runPartitioner(conf)) {
			int result = runPartitionerJob();
			if (result != 0)
				return result;

		}
		if (WikipediaConfiguration.runIngest(conf)) {
			int result = runIngestJob();
			if (result != 0)
				return result;

			if (WikipediaConfiguration.bulkIngest(conf))
				return loadBulkFiles();

		}
		return 0;
	}

	private int runPartitionerJob() throws Exception {
		Job partitionerJob = new Job(getConf(), "Partition Wikipedia");
		Configuration partitionerConf = partitionerJob.getConfiguration();
		partitionerConf.set("mapred.map.tasks.speculative.execution", "false");
		configurePartitionerJob(partitionerJob);
		List<Path> inputPaths = new ArrayList<Path>();
		SortedSet<String> languages = new TreeSet<String>();
		FileSystem fs = FileSystem.get(partitionerConf);
		Path parent = new Path(partitionerConf.get("wikipedia.input"));
		listFiles(parent, fs, inputPaths, languages);
		System.out.println(((("Input files in " + parent) + ":") + (inputPaths.size())));
		Path[] inputPathsArray = new Path[inputPaths.size()];
		inputPaths.toArray(inputPathsArray);
		System.out.println(("Languages:" + (languages.size())));
		WikipediaInputFormat.setInputPaths(partitionerJob, inputPathsArray);
		partitionerJob.setMapperClass(WikipediaPartitioner.class);
		partitionerJob.setNumReduceTasks(0);
		partitionerJob.setMapOutputKeyClass(Text.class);
		partitionerJob.setMapOutputValueClass(ArticleExtractor.Article.class);
		partitionerJob.setOutputKeyClass(Text.class);
		partitionerJob.setOutputValueClass(ArticleExtractor.Article.class);
		partitionerJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		Path outputDir = WikipediaConfiguration.getPartitionedArticlesPath(partitionerConf);
		SequenceFileOutputFormat.setOutputPath(partitionerJob, outputDir);
		SequenceFileOutputFormat.setCompressOutput(partitionerJob, true);
		SequenceFileOutputFormat.setOutputCompressionType(partitionerJob, RECORD);
		return partitionerJob.waitForCompletion(true) ? 0 : 1;
	}

	private int runIngestJob() throws Exception {
		Job ingestJob = new Job(getConf(), "Ingest Partitioned Wikipedia");
		Configuration ingestConf = ingestJob.getConfiguration();
		ingestConf.set("mapred.map.tasks.speculative.execution", "false");
		configureIngestJob(ingestJob);
		String tablename = WikipediaConfiguration.getTableName(ingestConf);
		Connector connector = WikipediaConfiguration.getConnector(ingestConf);
		TableOperations tops = connector.tableOperations();
		createTables(tops, tablename);
		ingestJob.setMapperClass(WikipediaPartitionedMapper.class);
		ingestJob.setNumReduceTasks(0);
		ingestJob.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(ingestJob, WikipediaConfiguration.getPartitionedArticlesPath(ingestConf));
		SequenceFileInputFormat.setMinInputSplitSize(ingestJob, WikipediaConfiguration.getMinInputSplitSize(ingestConf));
		ingestJob.setMapOutputKeyClass(Text.class);
		ingestJob.setMapOutputValueClass(Mutation.class);
		if (WikipediaConfiguration.bulkIngest(ingestConf)) {
			ingestJob.setOutputFormatClass(SortingRFileOutputFormat.class);
			SortingRFileOutputFormat.setMaxBufferSize(ingestConf, WikipediaConfiguration.bulkIngestBufferSize(ingestConf));
			String bulkIngestDir = WikipediaConfiguration.bulkIngestDir(ingestConf);
			if (bulkIngestDir == null) {
				WikipediaPartitionedIngester.log.error("Bulk ingest dir not set");
				return 1;
			}
			SortingRFileOutputFormat.setPathName(ingestConf, WikipediaConfiguration.bulkIngestDir(ingestConf));
		}else {
			ingestJob.setOutputFormatClass(AccumuloOutputFormat.class);
			String zookeepers = WikipediaConfiguration.getZookeepers(ingestConf);
			String instanceName = WikipediaConfiguration.getInstanceName(ingestConf);
			String user = WikipediaConfiguration.getUser(ingestConf);
			byte[] password = WikipediaConfiguration.getPassword(ingestConf);
			AccumuloOutputFormat.setOutputInfo(ingestJob.getConfiguration(), user, password, true, tablename);
			AccumuloOutputFormat.setZooKeeperInstance(ingestJob.getConfiguration(), instanceName, zookeepers);
		}
		return ingestJob.waitForCompletion(true) ? 0 : 1;
	}

	private int loadBulkFiles() throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
		Configuration conf = getConf();
		Connector connector = WikipediaConfiguration.getConnector(conf);
		FileSystem fs = FileSystem.get(conf);
		String directory = WikipediaConfiguration.bulkIngestDir(conf);
		String failureDirectory = WikipediaConfiguration.bulkIngestFailureDir(conf);
		for (FileStatus status : fs.listStatus(new Path(directory))) {
			if ((status.isDir()) == false)
				continue;

			Path dir = status.getPath();
			Path failPath = new Path(((failureDirectory + "/") + (dir.getName())));
			fs.mkdirs(failPath);
			connector.tableOperations().importDirectory(dir.getName(), dir.toString(), failPath.toString(), true);
		}
		return 0;
	}

	public static final PathFilter partFilter = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return path.getName().startsWith("part");
		}
	};

	protected void configurePartitionerJob(Job job) {
		Configuration conf = job.getConfiguration();
		job.setJarByClass(WikipediaPartitionedIngester.class);
		job.setInputFormatClass(WikipediaInputFormat.class);
		conf.set(AggregatingRecordReader.START_TOKEN, "<page>");
		conf.set(AggregatingRecordReader.END_TOKEN, "</page>");
	}

	protected void configureIngestJob(Job job) {
		job.setJarByClass(WikipediaPartitionedIngester.class);
	}

	protected static final Pattern filePattern = Pattern.compile("([a-z_]+).*.xml(.bz2)?");

	protected void listFiles(Path path, FileSystem fs, List<Path> files, Set<String> languages) throws IOException {
		for (FileStatus status : fs.listStatus(path)) {
			if (status.isDir()) {
				listFiles(status.getPath(), fs, files, languages);
			}else {
				Path p = status.getPath();
				Matcher matcher = WikipediaPartitionedIngester.filePattern.matcher(p.getName());
				if (matcher.matches()) {
					languages.add(matcher.group(1));
					files.add(p);
				}
			}
		}
	}
}

