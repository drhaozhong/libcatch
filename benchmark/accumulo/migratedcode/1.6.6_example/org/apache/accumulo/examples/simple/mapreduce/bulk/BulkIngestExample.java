package org.apache.accumulo.examples.simple.mapreduce.bulk;


import com.beust.jcommander.Parameter;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.examples.simple.mapreduce.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BulkIngestExample extends Configured implements Tool {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();

		private Text outputValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context output) throws IOException, InterruptedException {
			int index = -1;
			for (int i = 0; i < (value.getLength()); i++) {
				if ((value.getBytes()[i]) == '\t') {
					index = i;
					break;
				}
			}
			if (index > 0) {
				outputKey.set(value.getBytes(), 0, index);
				outputValue.set(value.getBytes(), (index + 1), ((value.getLength()) - (index + 1)));
				output.write(outputKey, outputValue);
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Key, Value> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Key, Value>.Context output) throws IOException, InterruptedException {
			long timestamp = System.currentTimeMillis();
			int index = 0;
			for (Text value : values) {
				Key outputKey = new Key(key, new Text("colf"), new Text(String.format("col_%07d", index)), timestamp);
				index++;
				Value outputValue = new Value(value.getBytes(), 0, value.getLength());
				output.write(outputKey, outputValue);
			}
		}
	}

	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--inputDir", required = true)
		String inputDir;

		@Parameter(names = "--workDir", required = true)
		String workDir;
	}

	@Override
	public int run(String[] args) {
		BulkIngestExample.Opts opts = new BulkIngestExample.Opts();
		opts.parseArgs(BulkIngestExample.class.getName(), args);
		Configuration conf = getConf();
		PrintStream out = null;
		try {
			Job job = JobUtil.getJob(conf);
			job.setJobName("bulk ingest example");
			job.setJarByClass(this.getClass());
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(BulkIngestExample.MapClass.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(BulkIngestExample.ReduceClass.class);
			job.setOutputFormatClass(AccumuloFileOutputFormat.class);
			opts.getSecurePassword();
			Connector connector = opts.getConnector();
			TextInputFormat.setInputPaths(job, new Path(opts.inputDir));
			AccumuloFileOutputFormat.setOutputPath(job, new Path(((opts.workDir) + "/files")));
			FileSystem fs = FileSystem.get(conf);
			out = new PrintStream(new BufferedOutputStream(fs.create(new Path(((opts.workDir) + "/splits.txt")))));
			Collection<Text> splits = connector.tableOperations().listSplits(opts.tableName, 100);
			for (Text split : splits)
				out.println(Base64.encodeBase64String(TextUtil.getBytes(split)));

			job.setNumReduceTasks(((splits.size()) + 1));
			out.close();
			job.setPartitionerClass(RangePartitioner.class);
			RangePartitioner.setSplitFile(job, ((opts.workDir) + "/splits.txt"));
			job.waitForCompletion(true);
			Path failures = new Path(opts.workDir, "failures");
			fs.delete(failures, true);
			fs.mkdirs(new Path(opts.workDir, "failures"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (out != null)
				out.close();

		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BulkIngestExample(), args);
		System.exit(res);
	}
}

