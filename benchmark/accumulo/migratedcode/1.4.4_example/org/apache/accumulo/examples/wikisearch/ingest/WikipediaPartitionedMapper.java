package org.apache.accumulo.examples.wikisearch.ingest;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.AbstractMessageLite;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.wikisearch.ingest.ArticleExtractor.Article;
import org.apache.accumulo.examples.wikisearch.iterator.GlobalIndexUidCombiner;
import org.apache.accumulo.examples.wikisearch.normalizer.LcNoDiacriticsNormalizer;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import static org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.newBuilder;


public class WikipediaPartitionedMapper extends Mapper<Text, ArticleExtractor.Article, Text, Mutation> {
	public static final Charset UTF8 = Charset.forName("UTF-8");

	public static final String DOCUMENT_COLUMN_FAMILY = "d";

	public static final String METADATA_EVENT_COLUMN_FAMILY = "e";

	public static final String METADATA_INDEX_COLUMN_FAMILY = "i";

	public static final String TOKENS_FIELD_NAME = "TEXT";

	private static final Value NULL_VALUE = new Value(new byte[0]);

	private static final String cvPrefix = "all|";

	private int numPartitions = 0;

	private Text tablename = null;

	private Text indexTableName = null;

	private Text reverseIndexTableName = null;

	private Text metadataTableName = null;

	private static class MutationInfo {
		final String row;

		final String colfam;

		final String colqual;

		final ColumnVisibility cv;

		final long timestamp;

		public MutationInfo(String row, String colfam, String colqual, ColumnVisibility cv, long timestamp) {
			super();
			this.row = row;
			this.colfam = colfam;
			this.colqual = colqual;
			this.cv = cv;
			this.timestamp = timestamp;
		}

		@Override
		public boolean equals(Object obj) {
			WikipediaPartitionedMapper.MutationInfo other = ((WikipediaPartitionedMapper.MutationInfo) (obj));
			return ((((((row) == (other.row)) || (row.equals(other.row))) && (((colfam) == (other.colfam)) || (colfam.equals(other.colfam)))) && (colqual.equals(other.colqual))) && (((cv) == (other.cv)) || (cv.equals(other.cv)))) && ((timestamp) == (other.timestamp));
		}

		@Override
		public int hashCode() {
			return ((((row.hashCode()) ^ (colfam.hashCode())) ^ (colqual.hashCode())) ^ (cv.hashCode())) ^ ((int) (timestamp));
		}
	}

	private LRUOutputCombiner<WikipediaPartitionedMapper.MutationInfo, WikipediaPartitionedMapper.CountAndSet> wikiIndexOutput;

	private LRUOutputCombiner<WikipediaPartitionedMapper.MutationInfo, WikipediaPartitionedMapper.CountAndSet> wikiReverseIndexOutput;

	private LRUOutputCombiner<WikipediaPartitionedMapper.MutationInfo, Value> wikiMetadataOutput;

	private static class CountAndSet {
		public int count;

		public HashSet<String> set;

		public CountAndSet(String entry) {
			set = new HashSet<String>();
			set.add(entry);
			count = 1;
		}
	}

	MultiTableBatchWriter mtbw;

	@Override
	public void setup(final Mapper<Text, ArticleExtractor.Article, Text, Mutation>.Context context) {
		Configuration conf = context.getConfiguration();
		tablename = new Text(WikipediaConfiguration.getTableName(conf));
		indexTableName = new Text(((tablename) + "Index"));
		reverseIndexTableName = new Text(((tablename) + "ReverseIndex"));
		metadataTableName = new Text(((tablename) + "Metadata"));
		try {
			mtbw = WikipediaConfiguration.getConnector(conf).createMultiTableBatchWriter(10000000, 1000, 10);
		} catch (AccumuloException e) {
			throw new RuntimeException(e);
		} catch (AccumuloSecurityException e) {
			throw new RuntimeException(e);
		}
		final Text metadataTableNameFinal = metadataTableName;
		final Text indexTableNameFinal = indexTableName;
		final Text reverseIndexTableNameFinal = reverseIndexTableName;
		numPartitions = WikipediaConfiguration.getNumPartitions(conf);
		LRUOutputCombiner.Fold<WikipediaPartitionedMapper.CountAndSet> indexFold = new LRUOutputCombiner.Fold<WikipediaPartitionedMapper.CountAndSet>() {
			@Override
			public WikipediaPartitionedMapper.CountAndSet fold(WikipediaPartitionedMapper.CountAndSet oldValue, WikipediaPartitionedMapper.CountAndSet newValue) {
				oldValue.count += newValue.count;
				if (((oldValue.set) == null) || ((newValue.set) == null)) {
					oldValue.set = null;
					return oldValue;
				}
				oldValue.set.addAll(newValue.set);
				if ((oldValue.set.size()) > (GlobalIndexUidCombiner.MAX))
					oldValue.set = null;

				return oldValue;
			}
		};
		LRUOutputCombiner.Output<WikipediaPartitionedMapper.MutationInfo, WikipediaPartitionedMapper.CountAndSet> indexOutput = new LRUOutputCombiner.Output<WikipediaPartitionedMapper.MutationInfo, WikipediaPartitionedMapper.CountAndSet>() {
			@Override
			public void output(WikipediaPartitionedMapper.MutationInfo key, WikipediaPartitionedMapper.CountAndSet value) {
				Uid.List.Builder builder = newBuilder();
				builder.setCOUNT(value.count);
				if ((value.set) == null) {
					builder.setIGNORE(true);
					builder.clearUID();
				}else {
					builder.setIGNORE(false);
					builder.addAllUID(value.set);
				}
				Uid.List list = builder.build();
				Value val = new Value(list.toByteArray());
				Mutation m = new Mutation(key.row);
				m.put(key.colfam, key.colqual, key.cv, key.timestamp, val);
				try {
					mtbw.getBatchWriter(indexTableNameFinal.toString()).addMutation(m);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};
		LRUOutputCombiner.Output<WikipediaPartitionedMapper.MutationInfo, WikipediaPartitionedMapper.CountAndSet> reverseIndexOutput = new LRUOutputCombiner.Output<WikipediaPartitionedMapper.MutationInfo, WikipediaPartitionedMapper.CountAndSet>() {
			@Override
			public void output(WikipediaPartitionedMapper.MutationInfo key, WikipediaPartitionedMapper.CountAndSet value) {
				Uid.List.Builder builder = newBuilder();
				builder.setCOUNT(value.count);
				if ((value.set) == null) {
					builder.setIGNORE(true);
					builder.clearUID();
				}else {
					builder.setIGNORE(false);
					builder.addAllUID(value.set);
				}
				Uid.List list = builder.build();
				Value val = new Value(list.toByteArray());
				Mutation m = new Mutation(key.row);
				m.put(key.colfam, key.colqual, key.cv, key.timestamp, val);
				try {
					mtbw.getBatchWriter(reverseIndexTableNameFinal.toString()).addMutation(m);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};
		wikiIndexOutput = new LRUOutputCombiner<WikipediaPartitionedMapper.MutationInfo, WikipediaPartitionedMapper.CountAndSet>(10000, indexFold, indexOutput);
		wikiReverseIndexOutput = new LRUOutputCombiner<WikipediaPartitionedMapper.MutationInfo, WikipediaPartitionedMapper.CountAndSet>(10000, indexFold, reverseIndexOutput);
		wikiMetadataOutput = new LRUOutputCombiner<WikipediaPartitionedMapper.MutationInfo, Value>(10000, new LRUOutputCombiner.Fold<Value>() {
			@Override
			public Value fold(Value oldValue, Value newValue) {
				return oldValue;
			}
		}, new LRUOutputCombiner.Output<WikipediaPartitionedMapper.MutationInfo, Value>() {
			@Override
			public void output(WikipediaPartitionedMapper.MutationInfo key, Value value) {
				Mutation m = new Mutation(key.row);
				m.put(key.colfam, key.colqual, key.cv, key.timestamp, value);
				try {
					mtbw.getBatchWriter(metadataTableNameFinal.toString()).addMutation(m);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

	@Override
	protected void cleanup(Mapper<Text, ArticleExtractor.Article, Text, Mutation>.Context context) throws IOException, InterruptedException {
		wikiIndexOutput.flush();
		wikiMetadataOutput.flush();
		wikiReverseIndexOutput.flush();
		try {
			mtbw.close();
		} catch (MutationsRejectedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void map(Text language, ArticleExtractor.Article article, Mapper<Text, ArticleExtractor.Article, Text, Mutation>.Context context) throws IOException, InterruptedException {
		String NULL_BYTE = "\u0000";
		String colfPrefix = (language.toString()) + NULL_BYTE;
		String indexPrefix = "fi" + NULL_BYTE;
		ColumnVisibility cv = new ColumnVisibility(((WikipediaPartitionedMapper.cvPrefix) + language));
		if (article != null) {
			Text partitionId = new Text(Integer.toString(WikipediaMapper.getPartitionId(article, numPartitions)));
			Mutation m = new Mutation(partitionId);
			for (Map.Entry<String, Object> entry : article.getFieldValues().entrySet()) {
				m.put((colfPrefix + (article.getId())), (((entry.getKey()) + NULL_BYTE) + (entry.getValue().toString())), cv, article.getTimestamp(), WikipediaPartitionedMapper.NULL_VALUE);
				WikipediaPartitionedMapper.MutationInfo mm = new WikipediaPartitionedMapper.MutationInfo(entry.getKey(), WikipediaPartitionedMapper.METADATA_EVENT_COLUMN_FAMILY, language.toString(), cv, article.getTimestamp());
				wikiMetadataOutput.put(mm, WikipediaPartitionedMapper.NULL_VALUE);
			}
			Set<String> tokens = WikipediaMapper.getTokens(article);
			Multimap<String, String> indexFields = HashMultimap.create();
			LcNoDiacriticsNormalizer normalizer = new LcNoDiacriticsNormalizer();
			for (Map.Entry<String, String> index : article.getNormalizedFieldValues().entrySet())
				indexFields.put(index.getKey(), index.getValue());

			for (String token : tokens)
				indexFields.put(WikipediaPartitionedMapper.TOKENS_FIELD_NAME, normalizer.normalizeFieldValue("", token));

			for (Map.Entry<String, String> index : indexFields.entries()) {
				m.put((indexPrefix + (index.getKey())), ((((index.getValue()) + NULL_BYTE) + colfPrefix) + (article.getId())), cv, article.getTimestamp(), WikipediaPartitionedMapper.NULL_VALUE);
				WikipediaPartitionedMapper.MutationInfo gm = new WikipediaPartitionedMapper.MutationInfo(index.getValue(), index.getKey(), ((partitionId + NULL_BYTE) + language), cv, article.getTimestamp());
				wikiIndexOutput.put(gm, new WikipediaPartitionedMapper.CountAndSet(Integer.toString(article.getId())));
				WikipediaPartitionedMapper.MutationInfo grm = new WikipediaPartitionedMapper.MutationInfo(StringUtils.reverse(index.getValue()), index.getKey(), ((partitionId + NULL_BYTE) + language), cv, article.getTimestamp());
				wikiReverseIndexOutput.put(grm, new WikipediaPartitionedMapper.CountAndSet(Integer.toString(article.getId())));
				WikipediaPartitionedMapper.MutationInfo mm = new WikipediaPartitionedMapper.MutationInfo(index.getKey(), WikipediaPartitionedMapper.METADATA_INDEX_COLUMN_FAMILY, ((language + NULL_BYTE) + (LcNoDiacriticsNormalizer.class.getName())), cv, article.getTimestamp());
				wikiMetadataOutput.put(mm, WikipediaPartitionedMapper.NULL_VALUE);
			}
			m.put(WikipediaPartitionedMapper.DOCUMENT_COLUMN_FAMILY, (colfPrefix + (article.getId())), cv, article.getTimestamp(), new Value(Base64.encodeBase64(article.getText().getBytes())));
			context.write(tablename, m);
		}else {
			context.getCounter("wikipedia", "invalid articles").increment(1);
		}
		context.progress();
	}
}

