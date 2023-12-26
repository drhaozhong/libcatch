package org.apache.accumulo.examples.wikisearch.ingest;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.accumulo.examples.wikisearch.ingest.ArticleExtractor.Article;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WikipediaPartitioner extends Mapper<LongWritable, Text, Text, ArticleExtractor.Article> {
	public static final Charset UTF8 = Charset.forName("UTF-8");

	public static final String DOCUMENT_COLUMN_FAMILY = "d";

	public static final String METADATA_EVENT_COLUMN_FAMILY = "e";

	public static final String METADATA_INDEX_COLUMN_FAMILY = "i";

	public static final String TOKENS_FIELD_NAME = "TEXT";

	private static final Pattern languagePattern = Pattern.compile("([a-z_]+).*.xml(.bz2)?");

	private ArticleExtractor extractor;

	private String language;

	private int myGroup = -1;

	private int numGroups = -1;

	@Override
	public void setup(Mapper<LongWritable, Text, Text, ArticleExtractor.Article>.Context context) {
		Configuration conf = context.getConfiguration();
		WikipediaInputFormat.WikipediaInputSplit wiSplit = ((WikipediaInputFormat.WikipediaInputSplit) (context.getInputSplit()));
		myGroup = wiSplit.getPartition();
		numGroups = WikipediaConfiguration.getNumGroups(conf);
		FileSplit split = wiSplit.getFileSplit();
		String fileName = split.getPath().getName();
		Matcher matcher = WikipediaPartitioner.languagePattern.matcher(fileName);
		if (matcher.matches()) {
			language = matcher.group(1).replace('_', '-').toLowerCase();
		}else {
			throw new RuntimeException(("Unknown ingest language! " + fileName));
		}
		extractor = new ArticleExtractor();
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ArticleExtractor.Article>.Context context) throws IOException, InterruptedException {
		ArticleExtractor.Article article = extractor.extract(new InputStreamReader(new ByteArrayInputStream(value.getBytes()), WikipediaPartitioner.UTF8));
		if (article != null) {
			int groupId = WikipediaMapper.getPartitionId(article, numGroups);
			if (groupId != (myGroup))
				return;

			context.write(new Text(language), article);
		}else {
			context.getCounter("wikipedia", "invalid articles").increment(1);
			context.progress();
		}
	}
}

