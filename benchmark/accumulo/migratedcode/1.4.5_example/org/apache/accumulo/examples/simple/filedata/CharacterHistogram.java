package org.apache.accumulo.examples.simple.filedata;


import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.SummingArrayCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CharacterHistogram extends Configured implements Tool {
	public static final String VIS = "vis";

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(CachedConfiguration.getInstance(), new CharacterHistogram(), args));
	}

	public static class HistMapper extends Mapper<List<Map.Entry<Key, Value>>, InputStream, Text, Mutation> {
		private ColumnVisibility cv;

		public void map(List<Map.Entry<Key, Value>> k, InputStream v, Mapper<List<Map.Entry<Key, Value>>, InputStream, Text, Mutation>.Context context) throws IOException, InterruptedException {
			Long[] hist = new Long[256];
			for (int i = 0; i < (hist.length); i++)
				hist[i] = 0L;

			int b = v.read();
			while (b >= 0) {
				hist[b] += 1L;
				b = v.read();
			} 
			v.close();
			Mutation m = new Mutation(k.get(0).getKey().getRow());
			m.put("info", "hist", cv, new Value(SummingArrayCombiner.STRING_ARRAY_ENCODER.encode(Arrays.asList(hist))));
			context.write(new Text(), m);
		}

		@Override
		protected void setup(Mapper<List<Map.Entry<Key, Value>>, InputStream, Text, Mutation>.Context context) throws IOException, InterruptedException {
			cv = new ColumnVisibility(context.getConfiguration().get(CharacterHistogram.VIS, ""));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), this.getClass().getSimpleName());
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(ChunkInputFormat.class);
		ChunkInputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
		ChunkInputFormat.setInputInfo(job.getConfiguration(), args[2], args[3].getBytes(), args[4], new Authorizations(args[5].split(",")));
		job.getConfiguration().set(CharacterHistogram.VIS, args[6]);
		job.setMapperClass(CharacterHistogram.HistMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), args[2], args[3].getBytes(), false, args[4]);
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}
}

