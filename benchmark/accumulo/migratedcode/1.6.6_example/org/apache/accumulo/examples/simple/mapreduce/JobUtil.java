package org.apache.accumulo.examples.simple.mapreduce;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;


public class JobUtil {
	public static Job getJob(Configuration conf) throws IOException {
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		return job;
	}
}

