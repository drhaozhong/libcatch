package org.apache.accumulo.examples.wikisearch.output;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


final class BufferingRFileRecordWriter extends RecordWriter<Text, Mutation> {
	private final long maxSize;

	private final AccumuloConfiguration acuconf;

	private final Configuration conf;

	private final String filenamePrefix;

	private final String taskID;

	private final FileSystem fs;

	private int fileCount = 0;

	private long size;

	private Map<Text, TreeMap<Key, Value>> buffers = new HashMap<Text, TreeMap<Key, Value>>();

	private Map<Text, Long> bufferSizes = new HashMap<Text, Long>();

	private TreeMap<Key, Value> getBuffer(Text tablename) {
		TreeMap<Key, Value> buffer = buffers.get(tablename);
		if (buffer == null) {
			buffer = new TreeMap<Key, Value>();
			buffers.put(tablename, buffer);
			bufferSizes.put(tablename, 0L);
		}
		return buffer;
	}

	private Text getLargestTablename() {
		long max = 0;
		Text table = null;
		for (Map.Entry<Text, Long> e : bufferSizes.entrySet()) {
			if ((e.getValue()) > max) {
				max = e.getValue();
				table = e.getKey();
			}
		}
		return table;
	}

	private void flushLargestTable() throws IOException {
		Text tablename = getLargestTablename();
		if (tablename == null)
			return;

		long bufferSize = bufferSizes.get(tablename);
		TreeMap<Key, Value> buffer = buffers.get(tablename);
		if ((buffer.size()) == 0)
			return;

		String file = (((((((filenamePrefix) + "/") + tablename) + "/") + (taskID)) + "_") + ((fileCount)++)) + ".rf";
		FileSKVWriter writer = RFileOperations.getInstance().openWriter(file, fs, conf, acuconf);
		writer.startDefaultLocalityGroup();
		for (Map.Entry<Key, Value> e : buffer.entrySet()) {
			writer.append(e.getKey(), e.getValue());
		}
		writer.close();
		size -= bufferSize;
		buffer.clear();
		bufferSizes.put(tablename, 0L);
	}

	BufferingRFileRecordWriter(long maxSize, AccumuloConfiguration acuconf, Configuration conf, String filenamePrefix, String taskID, FileSystem fs) {
		this.maxSize = maxSize;
		this.acuconf = acuconf;
		this.conf = conf;
		this.filenamePrefix = filenamePrefix;
		this.taskID = taskID;
		this.fs = fs;
	}

	@Override
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		while ((size) > 0)
			flushLargestTable();

	}

	@Override
	public void write(Text table, Mutation mutation) throws IOException, InterruptedException {
		TreeMap<Key, Value> buffer = getBuffer(table);
		int mutationSize = 0;
		for (ColumnUpdate update : mutation.getUpdates()) {
			Key k = new Key(mutation.getRow(), update.getColumnFamily(), update.getColumnQualifier(), update.getColumnVisibility(), update.getTimestamp(), update.isDeleted());
			Value v = new Value(update.getValue());
			mutationSize += k.getSize();
			mutationSize += v.getSize();
			buffer.put(k, v);
		}
		size += mutationSize;
		long bufferSize = bufferSizes.get(table);
		bufferSize += mutationSize;
		bufferSizes.put(table, bufferSize);
		while ((size) >= (maxSize)) {
			flushLargestTable();
		} 
	}
}

