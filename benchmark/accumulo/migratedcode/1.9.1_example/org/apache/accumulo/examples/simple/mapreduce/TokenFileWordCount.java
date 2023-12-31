package org.apache.accumulo.examples.simple.mapreduce;


import java.io.IOException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapred.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TokenFileWordCount extends Configured implements Tool {
	private static final Logger log = LoggerFactory.getLogger(TokenFileWordCount.class);

	public static class MapClass extends Mapper<LongWritable, Text, Text, Mutation> {
		@Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Mutation>.Context output) throws IOException {
			String[] words = value.toString().split("\\s+");
			for (String word : words) {
				Mutation mutation = new Mutation(new Text(word));
				mutation.put(new Text("count"), new Text("20080906"), new Value("1".getBytes()));
				try {
					output.write(null, mutation);
				} catch (InterruptedException e) {
					TokenFileWordCount.log.error("Could not write to Context.", e);
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String instance = args[0];
		String zookeepers = args[1];
		String user = args[2];
		String tokenFile = args[3];
		String input = args[4];
		String tableName = args[5];
		Job job = getInstance(getConf());
		job.setJobName(TokenFileWordCount.class.getName());
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, input);
		job.setMapperClass(TokenFileWordCount.MapClass.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		AccumuloOutputFormat.setZooKeeperInstance(job, ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(zookeepers));
		AccumuloOutputFormat.setConnectorInfo(job, user, tokenFile);
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setDefaultTableName(job, tableName);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TokenFileWordCount(), args);
		System.exit(res);
	}
}

