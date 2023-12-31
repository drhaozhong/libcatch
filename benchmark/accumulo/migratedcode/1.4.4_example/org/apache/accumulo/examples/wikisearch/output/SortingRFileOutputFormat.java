package org.apache.accumulo.examples.wikisearch.output;


import java.io.IOException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;


public class SortingRFileOutputFormat extends OutputFormat<Text, Mutation> {
	public static final String PATH_NAME = "sortingrfileoutputformat.path";

	public static final String MAX_BUFFER_SIZE = "sortingrfileoutputformat.max.buffer.size";

	public static void setPathName(Configuration conf, String path) {
		conf.set(SortingRFileOutputFormat.PATH_NAME, path);
	}

	public static String getPathName(Configuration conf) {
		return conf.get(SortingRFileOutputFormat.PATH_NAME);
	}

	public static void setMaxBufferSize(Configuration conf, long maxBufferSize) {
		conf.setLong(SortingRFileOutputFormat.MAX_BUFFER_SIZE, maxBufferSize);
	}

	public static long getMaxBufferSize(Configuration conf) {
		return conf.getLong(SortingRFileOutputFormat.MAX_BUFFER_SIZE, (-1));
	}

	@Override
	public void checkOutputSpecs(JobContext job) throws IOException, InterruptedException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		return new OutputCommitter() {
			@Override
			public void setupTask(TaskAttemptContext arg0) throws IOException {
			}

			@Override
			public void setupJob(JobContext arg0) throws IOException {
			}

			@Override
			public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
				return false;
			}

			@Override
			public void commitTask(TaskAttemptContext arg0) throws IOException {
			}

			@Override
			public void cleanupJob(JobContext arg0) throws IOException {
			}

			@Override
			public void abortTask(TaskAttemptContext arg0) throws IOException {
			}
		};
	}

	@Override
	public RecordWriter<Text, Mutation> getRecordWriter(TaskAttemptContext attempt) throws IOException, InterruptedException {
		final Configuration conf = attempt.getConfiguration();
		final String filenamePrefix = SortingRFileOutputFormat.getPathName(conf);
		final String taskID = attempt.getTaskAttemptID().toString();
		final long maxSize = SortingRFileOutputFormat.getMaxBufferSize(conf);
		final FileSystem fs = FileSystem.get(conf);
		final AccumuloConfiguration acuconf = AccumuloConfiguration.getDefaultConfiguration();
		return new BufferingRFileRecordWriter(maxSize, acuconf, conf, filenamePrefix, taskID, fs);
	}
}

