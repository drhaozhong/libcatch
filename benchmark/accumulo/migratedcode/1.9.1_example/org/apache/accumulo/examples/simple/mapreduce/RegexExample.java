package org.apache.accumulo.examples.simple.mapreduce;


import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapred.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
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
		@Override
		public void map(Key row, Value data, Mapper<Key, Value, Key, Value>.Context context) throws IOException, InterruptedException {
			context.write(row, data);
		}
	}

	static class Opts extends MapReduceClientOnRequiredTable {
		@Parameter(names = "--rowRegex")
		String rowRegex;

		@Parameter(names = "--columnFamilyRegex")
		String columnFamilyRegex;

		@Parameter(names = "--columnQualifierRegex")
		String columnQualifierRegex;

		@Parameter(names = "--valueRegex")
		String valueRegex;

		@Parameter(names = "--output", required = true)
		String destination;
	}

	@Override
	public int run(String[] args) throws Exception {
		RegexExample.Opts opts = new RegexExample.Opts();
		opts.parseArgs(getClass().getName(), args);
		Job job = getInstance(getConf());
		job.setJobName(getClass().getSimpleName());
		job.setJarByClass(getClass());
		job.setInputFormatClass(AccumuloInputFormat.class);
		opts.setAccumuloConfigs(job);
		IteratorSetting regex = new IteratorSetting(50, "regex", RegExFilter.class);
		RegExFilter.setRegexs(regex, opts.rowRegex, opts.columnFamilyRegex, opts.columnQualifierRegex, opts.valueRegex, false);
		AccumuloInputFormat.addIterator(job, regex);
		job.setMapperClass(RegexExample.RegexMapper.class);
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(opts.destination));
		System.out.println(("setRowRegex: " + (opts.rowRegex)));
		System.out.println(("setColumnFamilyRegex: " + (opts.columnFamilyRegex)));
		System.out.println(("setColumnQualifierRegex: " + (opts.columnQualifierRegex)));
		System.out.println(("setValueRegex: " + (opts.valueRegex)));
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RegexExample(), args);
		if (res != 0)
			System.exit(res);

	}
}

