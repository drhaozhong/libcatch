

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.function.Predicate;
import javax.swing.JList;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordCount extends Configured implements Tool {
	private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

	static final String KEYSPACE = "wordcount";

	static final String COLUMN_FAMILY = "input_words";

	static final String OUTPUT_REDUCER_VAR = "output_reducer";

	static final String OUTPUT_COLUMN_FAMILY = "output_words";

	private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count";

	private static final String CONF_COLUMN_NAME = "columnname";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(0);
	}

	public static class TokenizerMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, Cell>, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);

		private Text word = new Text();

		private ByteBuffer sourceColumn;

		protected void setup(Mapper.Context context) throws IOException, InterruptedException {
		}

		public void map(ByteBuffer key, TreeMap<ByteBuffer, Cell> columns, Mapper<ByteBuffer, SortedMap<ByteBuffer, Cell>, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			for (Cell cell : columns.values()) {
				String name = ByteBufferUtil.string(cell.name());
				String value = null;
				if (name.contains("int"))
					value = String.valueOf(ByteBufferUtil.toInt(cell.value()));
				else
					value = ByteBufferUtil.string(cell.value());

				WordCount.logger.debug("read {}:{}={} from {}", new Object[]{ ByteBufferUtil.string(key), name, value, context.getInputSplit() });
				StringTokenizer itr = new StringTokenizer(value);
				while (itr.hasMoreTokens()) {
					word.set(itr.nextToken());
					context.write(word, WordCount.TokenizerMapper.one);
				} 
			}
		}
	}

	public static class ReducerToFilesystem extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();

			context.write(key, new IntWritable(sum));
		}
	}

	public static class ReducerToCassandra extends Reducer<Text, IntWritable, ByteBuffer, JList<Duration>> {
		private ByteBuffer outputKey;

		protected void setup(Reducer.Context context) throws IOException, InterruptedException {
			outputKey = ByteBufferUtil.bytes(context.getConfiguration().get(WordCount.CONF_COLUMN_NAME));
		}

		public void reduce(Text word, Iterable<IntWritable> values, Reducer context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();

		}

		private static Duration getMutation(Text word, int sum) {
			Column c = new Column();
			c.setName(Arrays.copyOf(word.getBytes(), word.getLength()));
			c.setValue(ByteBufferUtil.bytes(sum));
			c.setTimestamp(System.currentTimeMillis());
			Mutation m = new Mutation();
			m.column_or_supercolumn.setColumn(c);
			return m;
		}
	}

	public int run(String[] args) throws Exception {
		String outputReducerType = "filesystem";
		if ((args != null) && (args[0].startsWith(WordCount.OUTPUT_REDUCER_VAR))) {
			String[] s = args[0].split("=");
			if ((s != null) && ((s.length) == 2))
				outputReducerType = s[1];

		}
		WordCount.logger.info(("output reducer type: " + outputReducerType));
		ConfigHelper.setRangeBatchSize(getConf(), 99);
		for (int i = 0; i < (WordCountSetup.TEST_COUNT); i++) {
			String columnName = "text" + i;
			Job job = new Job(getConf(), "wordcount");
			job.setJarByClass(WordCount.class);
			job.setMapperClass(WordCount.TokenizerMapper.class);
			if (outputReducerType.equalsIgnoreCase("filesystem")) {
				job.setCombinerClass(WordCount.ReducerToFilesystem.class);
				job.setReducerClass(WordCount.ReducerToFilesystem.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				FileOutputFormat.setOutputPath(job, new Path(((WordCount.OUTPUT_PATH_PREFIX) + i)));
			}else {
				job.setReducerClass(WordCount.ReducerToCassandra.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				job.setOutputKeyClass(ByteBuffer.class);
				job.setOutputValueClass(JList.class);
				job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
				ConfigHelper.setOutputColumnFamily(job.getConfiguration(), WordCount.KEYSPACE, WordCount.OUTPUT_COLUMN_FAMILY);
				job.getConfiguration().set(WordCount.CONF_COLUMN_NAME, "sum");
			}
			job.setInputFormatClass(ColumnFamilyInputFormat.class);
			ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
			ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
			ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
			ConfigHelper.setInputColumnFamily(job.getConfiguration(), WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
			Predicate predicate = new SlicePredicate().setColumn_names();
			if (i == 4) {
				IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("int4"), EQ, ByteBufferUtil.bytes(0));
			}
			if (i == 5) {
				ConfigHelper.setInputColumnFamily(job.getConfiguration(), WordCount.KEYSPACE, WordCount.COLUMN_FAMILY, true);
			}
			ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
			ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
			job.waitForCompletion(true);
		}
		return 0;
	}
}

