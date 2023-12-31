package org.apache.accumulo.examples.simple.mapreduce;


import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NGramIngest extends Configured implements Tool {
	private static final Logger log = LoggerFactory.getLogger(NGramIngest.class);

	static class Opts extends MapReduceClientOnRequiredTable {
		@Parameter(names = "--input", required = true)
		String inputDirectory;
	}

	static class NGramMapper extends Mapper<LongWritable, Text, Text, Mutation> {
		@Override
		protected void map(LongWritable location, Text value, Mapper<LongWritable, Text, Text, Mutation>.Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split("\\t");
			if ((parts.length) >= 4) {
				Mutation m = new Mutation(parts[0]);
				m.put(parts[1], String.format("%010d", Long.parseLong(parts[2])), new Value(parts[3].trim().getBytes()));
				context.write(null, m);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		NGramIngest.Opts opts = new NGramIngest.Opts();
		opts.parseArgs(getClass().getName(), args);
		Job job = getInstance(getConf());
		job.setJobName(getClass().getSimpleName());
		job.setJarByClass(getClass());
		opts.setAccumuloConfigs(job);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.class);
		job.setMapperClass(NGramIngest.NGramMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setNumReduceTasks(0);
		if (!(opts.getConnector().tableOperations().exists(opts.getTableName()))) {
			NGramIngest.log.info(("Creating table " + (opts.getTableName())));
			opts.getConnector().tableOperations().create(opts.getTableName());
			SortedSet<Text> splits = new TreeSet<>();
			String[] numbers = "1 2 3 4 5 6 7 8 9".split("\\s");
			String[] lower = "a b c d e f g h i j k l m n o p q r s t u v w x y z".split("\\s");
			String[] upper = "A B C D E F G H I J K L M N O P Q R S T U V W X Y Z".split("\\s");
			for (String[] array : new String[][]{ numbers, lower, upper }) {
				for (String s : array) {
					splits.add(new Text(s));
				}
			}
			opts.getConnector().tableOperations().addSplits(opts.getTableName(), splits);
		}
		TextInputFormat.addInputPath(job, new Path(opts.inputDirectory));
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NGramIngest(), args);
		if (res != 0)
			System.exit(res);

	}
}

