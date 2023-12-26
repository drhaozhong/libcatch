package org.apache.accumulo.examples.wikisearch.reader;


import java.io.IOException;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaConfiguration;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaInputFormat;
import org.apache.accumulo.examples.wikisearch.util.TextUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class AggregatingRecordReader extends LongLineRecordReader {
	public static final String START_TOKEN = "aggregating.token.start";

	public static final String END_TOKEN = "aggregating.token.end";

	public static final String RETURN_PARTIAL_MATCHES = "aggregating.allow.partial";

	private LongWritable key = new LongWritable();

	private String startToken = null;

	private String endToken = null;

	private long counter = 0;

	private Text aggValue = new Text();

	private boolean startFound = false;

	private StringBuilder remainder = new StringBuilder(0);

	private boolean returnPartialMatches = false;

	@Override
	public LongWritable getCurrentKey() {
		key.set(counter);
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return aggValue;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		super.initialize(((WikipediaInputFormat.WikipediaInputSplit) (genericSplit)).getFileSplit(), context);
		this.startToken = WikipediaConfiguration.isNull(context.getConfiguration(), AggregatingRecordReader.START_TOKEN, String.class);
		this.endToken = WikipediaConfiguration.isNull(context.getConfiguration(), AggregatingRecordReader.END_TOKEN, String.class);
		this.returnPartialMatches = context.getConfiguration().getBoolean(AggregatingRecordReader.RETURN_PARTIAL_MATCHES, false);
		byte[] txtBuffer = new byte[2048];
		aggValue.set(txtBuffer);
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		aggValue.clear();
		boolean hasNext = false;
		boolean finished = false;
		while ((!finished) && (((hasNext = super.nextKeyValue()) == true) || ((remainder.length()) > 0))) {
			if (hasNext)
				finished = process(super.getCurrentValue());
			else
				finished = process(null);

			if (finished) {
				startFound = false;
				(counter)++;
				return true;
			}
		} 
		if (((returnPartialMatches) && (startFound)) && ((aggValue.getLength()) > 0)) {
			startFound = false;
			(counter)++;
			return true;
		}
		return false;
	}

	private boolean process(Text t) {
		if (null != t)
			remainder.append(t.toString());

		while ((remainder.length()) > 0) {
			if (!(startFound)) {
				int start = remainder.indexOf(startToken);
				if ((-1) != start) {
					TextUtil.textAppendNoNull(aggValue, remainder.substring(start, (start + (startToken.length()))), false);
					remainder.delete(0, (start + (startToken.length())));
					startFound = true;
				}else {
					remainder.delete(0, remainder.length());
				}
			}else {
				int end = remainder.indexOf(endToken);
				int start = remainder.indexOf(startToken);
				if ((-1) == end) {
					if ((returnPartialMatches) && (start >= 0)) {
						TextUtil.textAppendNoNull(aggValue, remainder.substring(0, start), false);
						remainder.delete(0, start);
						return true;
					}else {
						TextUtil.textAppendNoNull(aggValue, remainder.toString(), false);
						remainder.delete(0, remainder.length());
					}
				}else {
					if (((returnPartialMatches) && (start >= 0)) && (start < end)) {
						TextUtil.textAppendNoNull(aggValue, remainder.substring(0, start), false);
						remainder.delete(0, start);
						return true;
					}else {
						TextUtil.textAppendNoNull(aggValue, remainder.substring(0, (end + (endToken.length()))), false);
						remainder.delete(0, (end + (endToken.length())));
						return true;
					}
				}
			}
		} 
		return false;
	}
}

