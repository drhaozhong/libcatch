

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.SortedMap;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordCountCounters extends Configured implements Tool {
	private static final Logger logger = LoggerFactory.getLogger(WordCountCounters.class);

	static final String COUNTER_COLUMN_FAMILY = "input_words_count";

	private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count_counters";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCountCounters(), args);
		System.exit(0);
	}

	public static class SumMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, LongWritable> {
		public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (IColumn column : columns.values()) {
				WordCountCounters.logger.debug(((((("read " + key) + ":") + (column.name())) + " from ") + (context.getInputSplit())));
				sum += ByteBufferUtil.toLong(column.value());
			}
			context.write(new Text(ByteBufferUtil.string(key)), new LongWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "wordcountcounters");
		job.setJarByClass(WordCountCounters.class);
		job.setMapperClass(WordCountCounters.SumMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(WordCountCounters.OUTPUT_PATH_PREFIX));
		job.setInputFormatClass(ColumnFamilyInputFormat.class);
		ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
		ConfigHelper.setInitialAddress(job.getConfiguration(), "localhost");
		ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), WordCount.KEYSPACE, WordCountCounters.COUNTER_COLUMN_FAMILY);
		SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER).setFinish(ByteBufferUtil.EMPTY_BYTE_BUFFER).setCount(100));
		ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
		job.waitForCompletion(true);
		return 0;
	}
}

