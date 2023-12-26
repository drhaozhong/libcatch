package org.apache.accumulo.examples.wikisearch.ingest;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.accumulo.examples.wikisearch.reader.AggregatingRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class WikipediaInputFormat extends TextInputFormat {
	public static class WikipediaInputSplit extends InputSplit implements Writable {
		public WikipediaInputSplit() {
		}

		public WikipediaInputSplit(FileSplit fileSplit, int partition) {
			this.fileSplit = fileSplit;
			this.partition = partition;
		}

		private FileSplit fileSplit = null;

		private int partition = -1;

		public int getPartition() {
			return partition;
		}

		public FileSplit getFileSplit() {
			return fileSplit;
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return fileSplit.getLength();
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return fileSplit.getLocations();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			Path file = new Path(in.readUTF());
			long start = in.readLong();
			long length = in.readLong();
			String[] hosts = null;
			if (in.readBoolean()) {
				int numHosts = in.readInt();
				hosts = new String[numHosts];
				for (int i = 0; i < numHosts; i++)
					hosts[i] = in.readUTF();

			}
			fileSplit = new FileSplit(file, start, length, hosts);
			partition = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(fileSplit.getPath().toString());
			out.writeLong(fileSplit.getStart());
			out.writeLong(fileSplit.getLength());
			String[] hosts = fileSplit.getLocations();
			if (hosts == null) {
				out.writeBoolean(false);
			}else {
				out.writeBoolean(true);
				out.writeInt(hosts.length);
				for (String host : hosts)
					out.writeUTF(host);

			}
			out.writeInt(partition);
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> superSplits = super.getSplits(job);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		int numGroups = WikipediaConfiguration.getNumGroups(job.getConfiguration());
		for (int group = 0; group < numGroups; group++) {
			for (InputSplit split : superSplits) {
				FileSplit fileSplit = ((FileSplit) (split));
				splits.add(new WikipediaInputFormat.WikipediaInputSplit(fileSplit, group));
			}
		}
		return splits;
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new AggregatingRecordReader();
	}
}

