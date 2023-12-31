package org.apache.accumulo.examples.simple.mapreduce;


import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RowHash extends Configured implements Tool {
	public static class HashDataMapper extends Mapper<Key, Value, Text, Mutation> {
		@Override
		public void map(Key row, Value data, Mapper<Key, Value, Text, Mutation>.Context context) throws IOException, InterruptedException {
			Mutation m = new Mutation(row.getRow());
			m.put(new Text("cf-HASHTYPE"), new Text("cq-MD5BASE64"), new Value(Base64.encodeBase64(MD5Hash.digest(data.toString()).getDigest())));
			context.write(null, m);
			context.progress();
		}

		@Override
		public void setup(Mapper<Key, Value, Text, Mutation>.Context job) {
		}
	}

	private static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--column", required = true)
		String column = null;
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobUtil.getJob(getConf());
		job.setJobName(this.getClass().getName());
		job.setJarByClass(this.getClass());
		RowHash.Opts opts = new RowHash.Opts();
		opts.parseArgs(RowHash.class.getName(), args);
		job.setInputFormatClass(AccumuloInputFormat.class);
		String col = opts.column;
		int idx = col.indexOf(":");
		Text cf = new Text((idx < 0 ? col : col.substring(0, idx)));
		Text cq = (idx < 0) ? null : new Text(col.substring((idx + 1)));
		if ((cf.getLength()) > 0)
			AccumuloInputFormat.fetchColumns(job, Collections.singleton(new Pair<Text, Text>(cf, cq)));

		job.setMapperClass(RowHash.HashDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RowHash(), args);
	}
}

