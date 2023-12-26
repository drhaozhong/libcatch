package org.apache.accumulo.examples.simple.mapreduce;


import com.beust.jcommander.Parameter;
import java.io.IOException;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool {
	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--input", description = "input directory")
		String inputDirectory;
	}

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
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		WordCount.Opts opts = new WordCount.Opts();
		opts.parseArgs(WordCount.class.getName(), args);
		Job job = JobUtil.getJob(getConf());
		job.setJobName(WordCount.class.getName());
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(opts.inputDirectory));
		job.setMapperClass(WordCount.MapClass.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		opts.setAccumuloConfigs(job);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCount(), args);
	}
}

