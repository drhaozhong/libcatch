package org.apache.accumulo.examples.simple.helloworld;


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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ietf.jgss.GSSManager;


public class InsertWithOutputFormat extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if ((args.length) != 5) {
			System.out.println((("Usage: bin/tool.sh " + (this.getClass().getName())) + " <instance name> <zoo keepers> <username> <password> <tablename>"));
			return 1;
		}
		Text tableName = new Text(args[4]);
		Job job = new Job(getConf());
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), args[2], args[3].getBytes(), true, null);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		TaskAttemptContext context = createContext(job);
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

