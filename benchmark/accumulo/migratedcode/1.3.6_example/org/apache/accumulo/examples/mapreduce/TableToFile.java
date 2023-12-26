package org.apache.accumulo.examples.mapreduce;


import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TableToFile extends Configured implements Tool {
	public static class TTFMapper extends Mapper<Key, Value, NullWritable, Text> {
		public void map(Key row, Value data, Mapper<Key, Value, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			final Key r = row;
			final Value v = data;
			Map.Entry<Key, Value> entry = new Map.Entry<Key, Value>() {
				@Override
				public Key getKey() {
					return r;
				}

				@Override
				public Value getValue() {
					return v;
				}

				@Override
				public Value setValue(Value value) {
					return null;
				}
			};
			context.write(NullWritable.get(), new Text(DefaultFormatter.formatEntry(entry, false)));
			context.setStatus("Outputed Value");
		}
	}

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(getConf(), (((this.getClass().getSimpleName()) + "_") + (System.currentTimeMillis())));
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(AccumuloInputFormat.class);
		AccumuloInputFormat.setInputInfo(job, args[2], args[3].getBytes(), args[4], new Authorizations());
		AccumuloInputFormat.setZooKeeperInstance(job, args[0], args[1]);
		HashSet<Pair<Text, Text>> columnsToFetch = new HashSet<Pair<Text, Text>>();
		for (String col : args[5].split(",")) {
			int idx = col.indexOf(":");
			Text cf = new Text((idx < 0 ? col : col.substring(0, idx)));
			Text cq = (idx < 0) ? null : new Text(col.substring((idx + 1)));
			columnsToFetch.add(new Pair<Text, Text>(cf, cq));
		}
		AccumuloInputFormat.fetchColumns(job, columnsToFetch);
		job.setMapperClass(TableToFile.TTFMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[6]));
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(), new TableToFile(), args);
		if (res != 0)
			System.exit(res);

	}
}

