package org.apache.accumulo.examples.simple.mapreduce;


import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
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
	static class Opts extends MapReduceClientOnRequiredTable {
		@Parameter(names = "--output", description = "output directory", required = true)
		String output;

		@Parameter(names = "--columns", description = "columns to extract, in cf:cq{,cf:cq,...} form")
		String columns = "";
	}

	public static class TTFMapper extends Mapper<Key, Value, NullWritable, Text> {
		@Override
		public void map(Key row, Value data, Mapper<Key, Value, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			Map.Entry<Key, Value> entry = new AbstractMap.SimpleImmutableEntry<Key, Value>(row, data);
			context.write(NullWritable.get(), new Text(DefaultFormatter.formatEntry(entry, false)));
			context.setStatus("Outputed Value");
		}
	}

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, AccumuloSecurityException {
		Job job = getInstance(getConf());
		job.setJobName((((this.getClass().getSimpleName()) + "_") + (System.currentTimeMillis())));
		job.setJarByClass(this.getClass());
		TableToFile.Opts opts = new TableToFile.Opts();
		opts.parseArgs(getClass().getName(), args);
		job.setInputFormatClass(AccumuloInputFormat.class);
		opts.setAccumuloConfigs(job);
		HashSet<Pair<Text, Text>> columnsToFetch = new HashSet<Pair<Text, Text>>();
		for (String col : opts.columns.split(",")) {
			int idx = col.indexOf(":");
			Text cf = new Text((idx < 0 ? col : col.substring(0, idx)));
			Text cq = (idx < 0) ? null : new Text(col.substring((idx + 1)));
			if ((cf.getLength()) > 0)
				columnsToFetch.add(new Pair<Text, Text>(cf, cq));

		}
		if (!(columnsToFetch.isEmpty()))
			AccumuloInputFormat.fetchColumns(job, columnsToFetch);

		job.setMapperClass(TableToFile.TTFMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(opts.output));
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TableToFile(), args);
	}
}

