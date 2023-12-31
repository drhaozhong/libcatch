package org.apache.accumulo.examples.mapreduce;


import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;


public class RowHash extends Configured implements Tool {
	public static class HashDataMapper extends Mapper<Key, Value, Text, Mutation> {
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

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), this.getClass().getName());
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(AccumuloInputFormat.class);
		AccumuloInputFormat.setZooKeeperInstance(job, args[0], args[1]);
		AccumuloInputFormat.setInputInfo(job, args[2], args[3].getBytes(), args[4], new Authorizations());
		String col = args[5];
		int idx = col.indexOf(":");
		Text cf = new Text((idx < 0 ? col : col.substring(0, idx)));
		Text cq = (idx < 0) ? null : new Text(col.substring((idx + 1)));
		AccumuloInputFormat.fetchColumns(job, Collections.singleton(new Pair<Text, Text>(cf, cq)));
		AccumuloInputFormat.setLogLevel(job, Level.TRACE);
		job.setMapperClass(RowHash.HashDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		AccumuloOutputFormat.setZooKeeperInstance(job, args[0], args[1]);
		AccumuloOutputFormat.setOutputInfo(job, args[2], args[3].getBytes(), true, args[6]);
		AccumuloOutputFormat.setLogLevel(job, Level.TRACE);
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(), new RowHash(), args);
		if (res != 0)
			System.exit(res);

	}
}

