package org.apache.accumulo.examples.simple.mapreduce;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
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
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			context.write(key, UniqueColumns.EMPTY);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if ((args.length) != 8) {
			throw new IllegalArgumentException((("Usage : " + (UniqueColumns.class.getSimpleName())) + " <instance name> <zookeepers> <user> <password> <table> <output directory> <num reducers> offline|online"));
		}
		boolean scanOffline = args[7].equals("offline");
		String table = args[4];
		String jobName = ((this.getClass().getSimpleName()) + "_") + (System.currentTimeMillis());
		Job job = new Job(getConf(), jobName);
		job.setJarByClass(this.getClass());
		String clone = table;
		Connector conn = null;
		if (scanOffline) {
			ZooKeeperInstance zki = new ZooKeeperInstance(args[0], args[1]);
			conn = zki.getConnector(args[2], args[3].getBytes());
			clone = (table + "_") + jobName;
			conn.tableOperations().clone(table, clone, true, new HashMap<String, String>(), new HashSet<String>());
			conn.tableOperations().offline(clone);
			AccumuloInputFormat.setScanOffline(job.getConfiguration(), true);
		}
		job.setInputFormatClass(AccumuloInputFormat.class);
		AccumuloInputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
		AccumuloInputFormat.setInputInfo(job.getConfiguration(), args[2], args[3].getBytes(), clone, new Authorizations());
		job.setMapperClass(UniqueColumns.UMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(UniqueColumns.UReducer.class);
		job.setReducerClass(UniqueColumns.UReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[6]));
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[5]));
		job.waitForCompletion(true);
		if (scanOffline) {
			conn.tableOperations().delete(clone);
		}
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(), new UniqueColumns(), args);
		System.exit(res);
	}
}

