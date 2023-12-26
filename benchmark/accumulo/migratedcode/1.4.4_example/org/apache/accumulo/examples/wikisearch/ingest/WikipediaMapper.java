package org.apache.accumulo.examples.wikisearch.ingest;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.AbstractMessageLite;
import java.awt.font.TextAttribute;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.attribute.FileAttribute;
import java.util.Collection;
import java.util.HashSet;
import java.util.IllegalFormatException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.wikisearch.normalizer.LcNoDiacriticsNormalizer;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
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
import org.apache.log4j.Category;
import org.apache.log4j.Logger;

import static org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.newBuilder;


public class WikipediaMapper extends Mapper<LongWritable, Text, Text, Mutation> {
	private static final Logger log = Logger.getLogger(WikipediaMapper.class);

	public static final Charset UTF8 = Charset.forName("UTF-8");

	public static final String DOCUMENT_COLUMN_FAMILY = "d";

	public static final String METADATA_EVENT_COLUMN_FAMILY = "e";

	public static final String METADATA_INDEX_COLUMN_FAMILY = "i";

	public static final String TOKENS_FIELD_NAME = "TEXT";

	private static final Pattern languagePattern = Pattern.compile("([a-z_]+).*.xml(.bz2)?");

	private static final Value NULL_VALUE = new Value(new byte[0]);

	private static final String cvPrefix = "all|";

	private ArticleExtractor extractor;

	private String language;

	private int numPartitions = 0;

	private ColumnVisibility cv = null;

	private int myGroup = -1;

	private int numGroups = -1;

	private Text tablename = null;

	private Text indexTableName = null;

	private Text reverseIndexTableName = null;

	private Text metadataTableName = null;

	@Override
	public void setup(Mapper<LongWritable, Text, Text, Mutation>.Context context) {
		Configuration conf = context.getConfiguration();
		tablename = new Text(WikipediaConfiguration.getTableName(conf));
		indexTableName = new Text(((tablename) + "Index"));
		reverseIndexTableName = new Text(((tablename) + "ReverseIndex"));
		metadataTableName = new Text(((tablename) + "Metadata"));
		WikipediaInputFormat.WikipediaInputSplit wiSplit = ((WikipediaInputFormat.WikipediaInputSplit) (context.getInputSplit()));
		myGroup = wiSplit.getPartition();
		numGroups = WikipediaConfiguration.getNumGroups(conf);
		FileSplit split = wiSplit.getFileSplit();
		String fileName = split.getPath().getName();
		Matcher matcher = WikipediaMapper.languagePattern.matcher(fileName);
		if (matcher.matches()) {
			language = matcher.group(1).replace('_', '-').toLowerCase();
		}else {
			throw new RuntimeException(("Unknown ingest language! " + fileName));
		}
		extractor = new ArticleExtractor();
		numPartitions = WikipediaConfiguration.getNumPartitions(conf);
		cv = new ColumnVisibility(((WikipediaMapper.cvPrefix) + (language)));
	}

	public static int getPartitionId(ArticleExtractor.Article article, int numPartitions) throws IllegalFormatException {
		return (article.getId()) % numPartitions;
	}

	static HashSet<String> metadataSent = new HashSet<String>();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Mutation>.Context context) throws IOException, InterruptedException {
		ArticleExtractor.Article article = extractor.extract(new InputStreamReader(new ByteArrayInputStream(value.getBytes()), WikipediaMapper.UTF8));
		String NULL_BYTE = "\u0000";
		String colfPrefix = (language) + NULL_BYTE;
		String indexPrefix = "fi" + NULL_BYTE;
		if (article != null) {
			int groupId = WikipediaMapper.getPartitionId(article, numGroups);
			if (groupId != (myGroup))
				return;

			Text partitionId = new Text(Integer.toString(WikipediaMapper.getPartitionId(article, numPartitions)));
			Mutation m = new Mutation(partitionId);
			for (Map.Entry<String, Object> entry : article.getFieldValues().entrySet()) {
				m.put((colfPrefix + (article.getId())), (((entry.getKey()) + NULL_BYTE) + (entry.getValue().toString())), cv, article.getTimestamp(), WikipediaMapper.NULL_VALUE);
				String metadataKey = ((entry.getKey()) + (WikipediaMapper.METADATA_EVENT_COLUMN_FAMILY)) + (language);
				if (!(WikipediaMapper.metadataSent.contains(metadataKey))) {
					Mutation mm = new Mutation(entry.getKey());
					mm.put(WikipediaMapper.METADATA_EVENT_COLUMN_FAMILY, language, cv, article.getTimestamp(), WikipediaMapper.NULL_VALUE);
					context.write(metadataTableName, mm);
					WikipediaMapper.metadataSent.add(metadataKey);
				}
			}
			Set<String> tokens = WikipediaMapper.getTokens(article);
			Multimap<String, String> indexFields = HashMultimap.create();
			LcNoDiacriticsNormalizer normalizer = new LcNoDiacriticsNormalizer();
			for (Map.Entry<String, String> index : article.getNormalizedFieldValues().entrySet())
				indexFields.put(index.getKey(), index.getValue());

			for (String token : tokens)
				indexFields.put(WikipediaMapper.TOKENS_FIELD_NAME, normalizer.normalizeFieldValue("", token));

			for (Map.Entry<String, String> index : indexFields.entries()) {
				m.put((indexPrefix + (index.getKey())), ((((index.getValue()) + NULL_BYTE) + colfPrefix) + (article.getId())), cv, article.getTimestamp(), WikipediaMapper.NULL_VALUE);
				Uid.List.Builder uidBuilder = newBuilder();
				uidBuilder.setIGNORE(false);
				uidBuilder.setCOUNT(1);
				uidBuilder.addUID(Integer.toString(article.getId()));
				Uid.List uidList = uidBuilder.build();
				Value val = new Value(uidList.toByteArray());
				Mutation gm = new Mutation(index.getValue());
				gm.put(index.getKey(), ((partitionId + NULL_BYTE) + (language)), cv, article.getTimestamp(), val);
				context.write(indexTableName, gm);
				Mutation grm = new Mutation(StringUtils.reverse(index.getValue()));
				grm.put(index.getKey(), ((partitionId + NULL_BYTE) + (language)), cv, article.getTimestamp(), val);
				context.write(reverseIndexTableName, grm);
				String metadataKey = ((index.getKey()) + (WikipediaMapper.METADATA_INDEX_COLUMN_FAMILY)) + (language);
				if (!(WikipediaMapper.metadataSent.contains(metadataKey))) {
					Mutation mm = new Mutation(index.getKey());
					mm.put(WikipediaMapper.METADATA_INDEX_COLUMN_FAMILY, (((language) + NULL_BYTE) + (LcNoDiacriticsNormalizer.class.getName())), cv, article.getTimestamp(), WikipediaMapper.NULL_VALUE);
					context.write(metadataTableName, mm);
					WikipediaMapper.metadataSent.add(metadataKey);
				}
			}
			m.put(WikipediaMapper.DOCUMENT_COLUMN_FAMILY, (colfPrefix + (article.getId())), cv, article.getTimestamp(), new Value(Base64.encodeBase64(article.getText().getBytes())));
			context.write(tablename, m);
		}else {
			context.getCounter("wikipedia", "invalid articles").increment(1);
		}
		context.progress();
	}

	static Set<String> getTokens(ArticleExtractor.Article article) throws IOException {
		Set<String> tokenList = new HashSet<String>();
		StreamTokenizer tok = new StreamTokenizer(new StringReader(article.getText()));
		FileAttribute term = addAttributes(TextAttribute.class);
		try {
			while (tok.parseNumbers()) {
				String token = term.name();
				if (!(StringUtils.isEmpty(token)))
					tokenList.add(token);

			} 
		} catch (IOException e) {
			WikipediaMapper.log.error("Error tokenizing text", e);
		} finally {
			try {
				tok.resetSyntax();
			} catch (IOException e) {
				WikipediaMapper.log.error("Error calling end()", e);
			} finally {
				try {
				} catch (IOException e) {
					WikipediaMapper.log.error("Error closing tokenizer", e);
				}
			}
		}
		return tokenList;
	}
}

