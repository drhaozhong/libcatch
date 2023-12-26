package org.apache.accumulo.examples.mapreduce;


import java.io.IOException;
import java.io.PrintStream;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RegexExample extends Configured implements Tool {
	public static class RegexMapper extends Mapper<Key, Value, Key, Value> {
		public void map(Key row, Value data, Mapper<Key, Value, Key, Value>.Context context) throws IOException, InterruptedException {
			context.write(row, data);
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), this.getClass().getSimpleName());
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(AccumuloInputFormat.class);
		AccumuloInputFormat.setInputInfo(job, args[2], args[3].getBytes(), args[4], new Authorizations());
		AccumuloInputFormat.setZooKeeperInstance(job, args[0], args[1]);
		job.setMapperClass(RegexExample.RegexMapper.class);
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[9]));
		System.out.println(("setRowRegex: " + (args[5])));
		System.out.println(("setColumnFamilyRegex: " + (args[6])));
		System.out.println(("setColumnQualifierRegex: " + (args[7])));
		System.out.println(("setValueRegex: " + (args[8])));
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(), new RegexExample(), args);
		if (res != 0)
			System.exit(res);

	}
}

