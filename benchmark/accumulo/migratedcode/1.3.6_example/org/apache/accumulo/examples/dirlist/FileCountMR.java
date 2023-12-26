package org.apache.accumulo.examples.dirlist;


import java.io.IOException;
import java.io.PrintStream;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FileCountMR extends Configured implements Tool {
	private static final String OUTPUT_VIS = (FileCountMR.class.getSimpleName()) + ".output.vis";

	private static final Text EMPTY = new Text();

	public static class FileCountMapper extends Mapper<Key, Value, Key, Value> {
		long dirCount = 0;

		long fileCount = 0;

		Text lastRowFound = null;

		String prefix;

		private void incrementCounts(Key k) {
			if (k.getColumnFamily().equals(QueryUtil.DIR_COLF))
				(dirCount)++;
			else
				(fileCount)++;

		}

		private void initVars(Key k) {
			lastRowFound = k.getRow();
			prefix = lastRowFound.toString();
			int slashIndex = prefix.lastIndexOf("/");
			if (slashIndex >= 0)
				prefix = prefix.substring(0, (slashIndex + 1));

			dirCount = 0;
			fileCount = 0;
			incrementCounts(k);
		}

		@Override
		protected void map(Key key, Value value, Mapper<Key, Value, Key, Value>.Context context) throws IOException, InterruptedException {
			if ((lastRowFound) == null) {
				initVars(key);
				return;
			}
			if (lastRowFound.equals(key.getRow())) {
				return;
			}
			if (((key.getRow().getLength()) > (prefix.length())) && (prefix.equals(key.getRow().toString().substring(0, prefix.length())))) {
				lastRowFound = key.getRow();
				incrementCounts(key);
				return;
			}
			cleanup(context);
			initVars(key);
		}

		@Override
		protected void cleanup(Mapper<Key, Value, Key, Value>.Context context) throws IOException, InterruptedException {
			if ((lastRowFound) == null)
				return;

			String lrf = lastRowFound.toString().substring(3);
			int slashIndex;
			int parentCount = 0;
			while ((slashIndex = lrf.lastIndexOf("/")) >= 0) {
				lrf = lrf.substring(0, slashIndex);
				context.write(new Key(lrf), new Value(StringArraySummation.longArrayToStringBytes((parentCount == 0 ? new Long[]{ dirCount, fileCount, dirCount, fileCount } : new Long[]{ 0L, 0L, dirCount, fileCount }))));
				parentCount++;
			} 
		}
	}

	public static class FileCountReducer extends Reducer<Key, Value, Text, Mutation> {
		ColumnVisibility colvis;

		StringArraySummation sas = new StringArraySummation();

		@Override
		protected void reduce(Key key, Iterable<Value> values, Reducer<Key, Value, Text, Mutation>.Context context) throws IOException, InterruptedException {
			sas.reset();
			for (Value v : values) {
				sas.collect(v);
			}
			Mutation m = new Mutation(QueryUtil.getRow(key.getRow().toString()));
			m.put(QueryUtil.DIR_COLF, QueryUtil.COUNTS_COLQ, colvis, sas.aggregate());
			context.write(FileCountMR.EMPTY, m);
		}

		@Override
		protected void setup(Reducer<Key, Value, Text, Mutation>.Context context) throws IOException, InterruptedException {
			colvis = new ColumnVisibility(context.getConfiguration().get(FileCountMR.OUTPUT_VIS, ""));
		}
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(CachedConfiguration.getInstance(), new FileCountMR(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		if ((args.length) < 8) {
			System.out.println((("usage: " + (FileCountMR.class.getSimpleName())) + " <instance> <zoo> <user> <pass> <input table> <output table> <auths> <output visibility>"));
			System.exit(1);
		}
		String instance = args[0];
		String zooKeepers = args[1];
		String user = args[2];
		String pass = args[3];
		String inputTable = args[4];
		String outputTable = args[5];
		Authorizations auths = new Authorizations(args[6].split(","));
		String colvis = args[7];
		Job job = new Job(getConf(), this.getClass().getSimpleName());
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(AccumuloInputFormat.class);
		AccumuloInputFormat.setInputInfo(job, user, pass.getBytes(), inputTable, auths);
		AccumuloInputFormat.setZooKeeperInstance(job, instance, zooKeepers);
		job.setMapperClass(FileCountMR.FileCountMapper.class);
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);
		job.setReducerClass(FileCountMR.FileCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		AccumuloOutputFormat.setOutputInfo(job, user, pass.getBytes(), true, outputTable);
		AccumuloOutputFormat.setZooKeeperInstance(job, instance, zooKeepers);
		job.setNumReduceTasks(5);
		job.getConfiguration().set(FileCountMR.OUTPUT_VIS, colvis);
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}
}

