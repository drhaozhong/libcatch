package org.apache.accumulo.examples.mapreduce.bulk;


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.BulkImportHelper;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
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
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Key, Value>.Context output) throws IOException, InterruptedException {
			long timestamp = System.currentTimeMillis();
			int index = 0;
			for (Text value : values) {
				Key outputKey = new Key(key, new Text("foo"), new Text(("" + index)), timestamp);
				index++;
				Value outputValue = new Value(value.getBytes(), 0, value.getLength());
				output.write(outputKey, outputValue);
			}
		}
	}

	public int run(String[] args) {
		if ((args.length) != 7) {
			System.out.println((("ERROR: Wrong number of parameters: " + (args.length)) + " instead of 7."));
			return printUsage();
		}
		Configuration conf = getConf();
		PrintStream out = null;
		try {
			Job job = new Job(conf, "bulk ingest example");
			job.setJarByClass(this.getClass());
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(BulkIngestExample.MapClass.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(BulkIngestExample.ReduceClass.class);
			job.setOutputFormatClass(AccumuloFileOutputFormat.class);
			AccumuloFileOutputFormat.setZooKeeperInstance(job, args[0], args[1]);
			Instance instance = new ZooKeeperInstance(args[0], args[1]);
			String user = args[2];
			byte[] pass = args[3].getBytes();
			String tableName = args[4];
			String inputDir = args[5];
			String workDir = args[6];
			Connector connector = instance.getConnector(user, pass);
			TextInputFormat.setInputPaths(job, new Path(inputDir));
			AccumuloFileOutputFormat.setOutputPath(job, new Path((workDir + "/files")));
			FileSystem fs = FileSystem.get(conf);
			out = new PrintStream(new BufferedOutputStream(fs.create(new Path((workDir + "/splits.txt")))));
			Collection<Text> splits = connector.tableOperations().getSplits(tableName, 100);
			for (Text split : splits)
				out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));

			job.setNumReduceTasks(((splits.size()) + 1));
			out.close();
			job.setPartitionerClass(RangePartitioner.class);
			RangePartitioner.setSplitFile(job, (workDir + "/splits.txt"));
			job.waitForCompletion(true);
			connector.tableOperations().importDirectory(tableName, (workDir + "/files"), (workDir + "/failures"), 20, 4, false);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (out != null)
				out.close();

		}
		return 0;
	}

	private int printUsage() {
		System.out.println((("accumulo " + (this.getClass().getName())) + " <instanceName> <zooKeepers> <username> <password> <table> <input dir> <work dir> "));
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(), new BulkIngestExample(), args);
		System.exit(res);
	}
}

