package org.apache.accumulo.examples.simple.mapreduce;


import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class UniqueColumns extends Configured implements Tool {
	private static final Text EMPTY = new Text();

	public static class UMapper extends Mapper<Key, Value, Text, Text> {
		private Text temp = new Text();

		private static final Text CF = new Text("cf:");

		private static final Text CQ = new Text("cq:");

		@Override
		public void map(Key key, Value value, Mapper<Key, Value, Text, Text>.Context context) throws IOException, InterruptedException {
			temp.set(UniqueColumns.UMapper.CF);
			ByteSequence cf = key.getColumnFamilyData();
			temp.append(cf.getBackingArray(), cf.offset(), cf.length());
			context.write(temp, UniqueColumns.EMPTY);
			temp.set(UniqueColumns.UMapper.CQ);
			ByteSequence cq = key.getColumnQualifierData();
			temp.append(cq.getBackingArray(), cq.offset(), cq.length());
			context.write(temp, UniqueColumns.EMPTY);
		}
	}

	public static class UReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			context.write(key, UniqueColumns.EMPTY);
		}
	}

	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--output", description = "output directory")
		String output;

		@Parameter(names = "--reducers", description = "number of reducers to use", required = true)
		int reducers;

		@Parameter(names = "--offline", description = "run against an offline table")
		boolean offline = false;
	}

	@Override
	public int run(String[] args) throws Exception {
		UniqueColumns.Opts opts = new UniqueColumns.Opts();
		opts.parseArgs(UniqueColumns.class.getName(), args);
		String jobName = ((this.getClass().getSimpleName()) + "_") + (System.currentTimeMillis());
		Job job = JobUtil.getJob(getConf());
		job.setJobName(jobName);
		job.setJarByClass(this.getClass());
		String clone = opts.tableName;
		Connector conn = null;
		opts.setAccumuloConfigs(job);
		if (opts.offline) {
			conn = opts.getConnector();
			clone = ((opts.tableName) + "_") + jobName;
			conn.tableOperations().clone(opts.tableName, clone, true, new HashMap<String, String>(), new HashSet<String>());
			conn.tableOperations().offline(clone);
			AccumuloInputFormat.setOfflineTableScan(job, true);
			AccumuloInputFormat.setInputTableName(job, clone);
		}
		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setMapperClass(UniqueColumns.UMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(UniqueColumns.UReducer.class);
		job.setReducerClass(UniqueColumns.UReducer.class);
		job.setNumReduceTasks(opts.reducers);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(opts.output));
		job.waitForCompletion(true);
		if (opts.offline) {
			conn.tableOperations().delete(clone);
		}
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new UniqueColumns(), args);
		System.exit(res);
	}
}

