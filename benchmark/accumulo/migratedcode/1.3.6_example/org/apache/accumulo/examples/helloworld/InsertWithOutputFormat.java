package org.apache.accumulo.examples.helloworld;


import java.io.PrintStream;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InsertWithOutputFormat extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if ((args.length) != 5) {
			System.out.println((("Usage: accumulo " + (this.getClass().getName())) + " <instance name> <zoo keepers> <tablename> <username> <password>"));
			return 1;
		}
		Text tableName = new Text(args[2]);
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		AccumuloOutputFormat.setZooKeeperInstance(job, args[0], args[1]);
		AccumuloOutputFormat.setOutputInfo(job, args[3], args[4].getBytes(), true, null);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		TaskAttemptContext context = new TaskAttemptContext(conf, new TaskAttemptID());
		RecordWriter<Text, Mutation> rw = new AccumuloOutputFormat().getRecordWriter(context);
		Text colf = new Text("colfam");
		System.out.println("writing ...");
		for (int i = 0; i < 10000; i++) {
			Mutation m = new Mutation(new Text(String.format("row_%d", i)));
			for (int j = 0; j < 5; j++) {
				m.put(colf, new Text(String.format("colqual_%d", j)), new Value(String.format("value_%d_%d", i, j).getBytes()));
			}
			rw.write(tableName, m);
			if ((i % 100) == 0)
				System.out.println(i);

		}
		rw.close(context);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(CachedConfiguration.getInstance(), new InsertWithOutputFormat(), args));
	}
}

