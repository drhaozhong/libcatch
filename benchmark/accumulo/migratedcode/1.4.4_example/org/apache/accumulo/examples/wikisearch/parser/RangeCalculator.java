package org.apache.accumulo.examples.wikisearch.parser;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.examples.wikisearch.iterator.EvaluatingIterator;
import org.apache.accumulo.examples.wikisearch.logic.AbstractQueryLogic;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.accumulo.examples.wikisearch.util.TextUtil;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;

import static org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.parseFrom;


public class RangeCalculator extends QueryParser {
	public static class MapKey implements Comparable<RangeCalculator.MapKey> {
		private String fieldName = null;

		private String fieldValue = null;

		private String originalQueryValue = null;

		public MapKey(String fieldName, String fieldValue) {
			super();
			this.fieldName = fieldName;
			this.fieldValue = fieldValue;
		}

		public String getFieldName() {
			return fieldName;
		}

		public String getFieldValue() {
			return fieldValue;
		}

		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}

		public void setFieldValue(String fieldValue) {
			this.fieldValue = fieldValue;
		}

		public String getOriginalQueryValue() {
			return originalQueryValue;
		}

		public void setOriginalQueryValue(String originalQueryValue) {
			this.originalQueryValue = originalQueryValue;
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder(17, 37).append(fieldName).append(fieldValue).toHashCode();
		}

		@Override
		public String toString() {
			return ((this.fieldName) + " ") + (this.fieldValue);
		}

		@Override
		public boolean equals(Object other) {
			if (other == null)
				return false;

			if (other instanceof RangeCalculator.MapKey) {
				RangeCalculator.MapKey o = ((RangeCalculator.MapKey) (other));
				return (this.fieldName.equals(o.fieldName)) && (this.fieldValue.equals(o.fieldValue));
			}else
				return false;

		}

		public int compareTo(RangeCalculator.MapKey o) {
			int result = this.fieldName.compareTo(o.fieldName);
			if (result != 0) {
				return this.fieldValue.compareTo(o.fieldValue);
			}else {
				return result;
			}
		}
	}

	public static class RangeBounds {
		private String originalLower = null;

		private Text lower = null;

		private String originalUpper = null;

		private Text upper = null;

		public Text getLower() {
			return lower;
		}

		public Text getUpper() {
			return upper;
		}

		public void setLower(Text lower) {
			this.lower = lower;
		}

		public void setUpper(Text upper) {
			this.upper = upper;
		}

		public String getOriginalLower() {
			return originalLower;
		}

		public String getOriginalUpper() {
			return originalUpper;
		}

		public void setOriginalLower(String originalLower) {
			this.originalLower = originalLower;
		}

		public void setOriginalUpper(String originalUpper) {
			this.originalUpper = originalUpper;
		}
	}

	protected static class TermRange implements Comparable<RangeCalculator.TermRange> {
		private String fieldName = null;

		private Object fieldValue = null;

		private Set<Range> ranges = new TreeSet<Range>();

		public TermRange(String name, Object fieldValue) {
			this.fieldName = name;
			this.fieldValue = fieldValue;
		}

		public String getFieldName() {
			return this.fieldName;
		}

		public Object getFieldValue() {
			return this.fieldValue;
		}

		public void addAll(Set<Range> r) {
			ranges.addAll(r);
		}

		public void add(Range r) {
			ranges.add(r);
		}

		public Set<Range> getRanges() {
			return ranges;
		}

		@Override
		public String toString() {
			ToStringBuilder tsb = new ToStringBuilder(this);
			tsb.append("fieldName", fieldName);
			tsb.append("fieldValue", fieldValue);
			tsb.append("ranges", ranges);
			return tsb.toString();
		}

		public int compareTo(RangeCalculator.TermRange o) {
			int result = this.fieldName.compareTo(o.fieldName);
			if (result == 0) {
				return ((Integer) (ranges.size())).compareTo(o.ranges.size());
			}else {
				return result;
			}
		}
	}

	static class EvaluationContext {
		boolean inOrContext = false;

		boolean inNotContext = false;

		boolean inAndContext = false;

		RangeCalculator.TermRange lastRange = null;

		String lastProcessedTerm = null;
	}

	protected static Logger log = Logger.getLogger(RangeCalculator.class);

	private static String WILDCARD = ".*";

	private static String SINGLE_WILDCARD = "\\.";

	protected Connector c;

	protected Authorizations auths;

	protected Multimap<String, Formatter> indexedTerms;

	protected Multimap<String, QueryParser.QueryTerm> termsCopy = HashMultimap.create();

	protected String indexTableName;

	protected String reverseIndexTableName;

	protected int queryThreads = 8;

	protected Set<Range> result = null;

	protected Multimap<String, String> indexEntries = HashMultimap.create();

	protected Map<String, String> indexValues = new HashMap<String, String>();

	protected Multimap<String, RangeCalculator.MapKey> originalQueryValues = HashMultimap.create();

	protected Map<String, Long> termCardinalities = new HashMap<String, Long>();

	protected Map<RangeCalculator.MapKey, RangeCalculator.TermRange> globalIndexResults = new HashMap<RangeCalculator.MapKey, RangeCalculator.TermRange>();

	public void execute(Connector c, Authorizations auths, Multimap<String, java.util.Formatter> indexedTerms, Multimap<String, QueryParser.QueryTerm> terms, String query, AbstractQueryLogic logic, Set<String> typeFilter) throws ParseException {
		super.execute(query);
		this.c = c;
		this.auths = auths;
		this.indexedTerms = indexedTerms;
		this.termsCopy.putAll(terms);
		this.indexTableName = logic.getIndexTableName();
		this.reverseIndexTableName = logic.getReverseIndexTableName();
		this.queryThreads = logic.getQueryThreads();
		Map<RangeCalculator.MapKey, Set<Range>> indexRanges = new HashMap<RangeCalculator.MapKey, Set<Range>>();
		Map<RangeCalculator.MapKey, Set<Range>> trailingWildcardRanges = new HashMap<RangeCalculator.MapKey, Set<Range>>();
		Map<RangeCalculator.MapKey, Set<Range>> leadingWildcardRanges = new HashMap<RangeCalculator.MapKey, Set<Range>>();
		Map<Text, RangeCalculator.RangeBounds> rangeMap = new HashMap<Text, RangeCalculator.RangeBounds>();
		for (Map.Entry<String, QueryParser.QueryTerm> entry : terms.entries()) {
			if ((((((entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTEQNode.class))) || (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTERNode.class)))) || (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTLTNode.class)))) || (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTLENode.class)))) || (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTGTNode.class)))) || (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTGENode.class)))) {
				if (!(indexedTerms.containsKey(entry.getKey()))) {
					termCardinalities.put(entry.getKey().toUpperCase(), 0L);
					continue;
				}
				if (null == (entry.getValue())) {
					termCardinalities.put(entry.getKey().toUpperCase(), 0L);
					continue;
				}
				if ((null == (entry.getValue().getValue())) || (((String) (entry.getValue().getValue())).equals("null"))) {
					termCardinalities.put(entry.getKey().toUpperCase(), 0L);
					continue;
				}
				String value = null;
				if ((((String) (entry.getValue().getValue())).startsWith("'")) && (((String) (entry.getValue().getValue())).endsWith("'")))
					value = ((String) (entry.getValue().getValue())).substring(1, ((((String) (entry.getValue().getValue())).length()) - 1));
				else
					value = ((String) (entry.getValue().getValue()));

				for (Formatter normalizer : indexedTerms.get(entry.getKey())) {
					String normalizedFieldValue = setFieldValue(null, value);
					Text fieldValue = new Text(normalizedFieldValue);
					Text fieldName = new Text(entry.getKey().toUpperCase());
					if (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTEQNode.class))) {
						Key startRange = new Key(fieldValue, fieldName);
						Range r = new Range(startRange, true, startRange.followingKey(PartialKey.ROW), true);
						RangeCalculator.MapKey key = new RangeCalculator.MapKey(fieldName.toString(), fieldValue.toString());
						key.setOriginalQueryValue(value);
						this.originalQueryValues.put(value, key);
						if (!(indexRanges.containsKey(key)))
							indexRanges.put(key, new HashSet<Range>());

						indexRanges.get(key).add(r);
					}else
						if (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTERNode.class))) {
							int loc = normalizedFieldValue.indexOf(RangeCalculator.WILDCARD);
							if ((-1) == loc)
								loc = normalizedFieldValue.indexOf(RangeCalculator.SINGLE_WILDCARD);

							if ((-1) == loc) {
								Key startRange = new Key(fieldValue, fieldName);
								Range r = new Range(startRange, true, startRange.followingKey(PartialKey.ROW), true);
								RangeCalculator.MapKey key = new RangeCalculator.MapKey(fieldName.toString(), fieldValue.toString());
								key.setOriginalQueryValue(value);
								this.originalQueryValues.put(value, key);
								if (!(indexRanges.containsKey(key)))
									indexRanges.put(key, new HashSet<Range>());

								indexRanges.get(key).add(r);
							}else {
								if (loc == 0) {
									StringBuilder buf = new StringBuilder(normalizedFieldValue.substring(2));
									normalizedFieldValue = buf.reverse().toString();
									Key startRange = new Key(new Text((normalizedFieldValue + "\u0000")), fieldName);
									Key endRange = new Key(new Text((normalizedFieldValue + "\u10ffFF")), fieldName);
									Range r = new Range(startRange, true, endRange, true);
									RangeCalculator.MapKey key = new RangeCalculator.MapKey(fieldName.toString(), normalizedFieldValue);
									key.setOriginalQueryValue(value);
									this.originalQueryValues.put(value, key);
									if (!(leadingWildcardRanges.containsKey(key)))
										leadingWildcardRanges.put(key, new HashSet<Range>());

									leadingWildcardRanges.get(key).add(r);
								}else
									if (loc == ((normalizedFieldValue.length()) - 2)) {
										normalizedFieldValue = normalizedFieldValue.substring(0, loc);
										Key startRange = new Key(new Text((normalizedFieldValue + "\u0000")), fieldName);
										Key endRange = new Key(new Text((normalizedFieldValue + "\u10ffFF")), fieldName);
										Range r = new Range(startRange, true, endRange, true);
										RangeCalculator.MapKey key = new RangeCalculator.MapKey(fieldName.toString(), normalizedFieldValue);
										key.setOriginalQueryValue(value);
										this.originalQueryValues.put(value, key);
										if (!(trailingWildcardRanges.containsKey(key)))
											trailingWildcardRanges.put(key, new HashSet<Range>());

										trailingWildcardRanges.get(key).add(r);
									}else {
									}

							}
						}else
							if ((entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTGTNode.class))) || (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTGENode.class)))) {
								if (!(rangeMap.containsKey(fieldName)))
									rangeMap.put(fieldName, new RangeCalculator.RangeBounds());

								rangeMap.get(fieldName).setLower(fieldValue);
								rangeMap.get(fieldName).setOriginalLower(value);
							}else
								if ((entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTLTNode.class))) || (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTLENode.class)))) {
									if (!(rangeMap.containsKey(fieldName)))
										rangeMap.put(fieldName, new RangeCalculator.RangeBounds());

									rangeMap.get(fieldName).setUpper(fieldValue);
									rangeMap.get(fieldName).setOriginalUpper(value);
								}



				}
			}
		}
		for (Map.Entry<Text, RangeCalculator.RangeBounds> entry : rangeMap.entrySet()) {
			if (((entry.getValue().getLower()) != null) && ((entry.getValue().getUpper()) != null)) {
				Key lk = new Key(entry.getValue().getLower());
				Key up = new Key(entry.getValue().getUpper());
				Text lower = lk.getRow();
				Text upper = up.getRow();
				if ((lk.compareTo(up)) > 0) {
					lower = up.getRow();
					upper = lk.getRow();
				}
				Key startRange = new Key(lower, entry.getKey());
				Key endRange = new Key(upper, entry.getKey());
				Range r = new Range(startRange, true, endRange, true);
				Map<RangeCalculator.MapKey, Set<Range>> ranges = new HashMap<RangeCalculator.MapKey, Set<Range>>();
				RangeCalculator.MapKey key = new RangeCalculator.MapKey(entry.getKey().toString(), entry.getValue().getLower().toString());
				key.setOriginalQueryValue(entry.getValue().getOriginalLower().toString());
				this.originalQueryValues.put(entry.getValue().getOriginalLower().toString(), key);
				ranges.put(key, new HashSet<Range>());
				ranges.get(key).add(r);
				try {
					Map<RangeCalculator.MapKey, RangeCalculator.TermRange> lowerResults = queryGlobalIndex(ranges, entry.getKey().toString(), this.indexTableName, false, key, typeFilter);
					Map<RangeCalculator.MapKey, RangeCalculator.TermRange> upperResults = new HashMap<RangeCalculator.MapKey, RangeCalculator.TermRange>();
					for (Map.Entry<RangeCalculator.MapKey, RangeCalculator.TermRange> e : lowerResults.entrySet()) {
						RangeCalculator.MapKey key2 = new RangeCalculator.MapKey(e.getKey().getFieldName(), entry.getValue().getUpper().toString());
						key2.setOriginalQueryValue(entry.getValue().getOriginalUpper().toString());
						upperResults.put(key2, e.getValue());
						this.originalQueryValues.put(entry.getValue().getOriginalUpper(), key2);
					}
					this.globalIndexResults.putAll(lowerResults);
					this.globalIndexResults.putAll(upperResults);
				} catch (TableNotFoundException e) {
					RangeCalculator.log.error("index table not found", e);
					throw new RuntimeException(" index table not found", e);
				}
			}else {
				RangeCalculator.log.warn(((("Unbounded range detected, not querying index for it. Field  " + (entry.getKey().toString())) + " in query: ") + query));
			}
		}
		try {
			for (Map.Entry<RangeCalculator.MapKey, Set<Range>> trailing : trailingWildcardRanges.entrySet()) {
				Map<RangeCalculator.MapKey, Set<Range>> m = new HashMap<RangeCalculator.MapKey, Set<Range>>();
				m.put(trailing.getKey(), trailing.getValue());
				if (RangeCalculator.log.isDebugEnabled())
					RangeCalculator.log.debug(("Ranges for Wildcard Global Index query: " + (m.toString())));

				this.globalIndexResults.putAll(queryGlobalIndex(m, trailing.getKey().getFieldName(), this.indexTableName, false, trailing.getKey(), typeFilter));
			}
			for (Map.Entry<RangeCalculator.MapKey, Set<Range>> leading : leadingWildcardRanges.entrySet()) {
				Map<RangeCalculator.MapKey, Set<Range>> m = new HashMap<RangeCalculator.MapKey, Set<Range>>();
				m.put(leading.getKey(), leading.getValue());
				if (RangeCalculator.log.isDebugEnabled())
					RangeCalculator.log.debug(("Ranges for Wildcard Global Reverse Index query: " + (m.toString())));

				this.globalIndexResults.putAll(queryGlobalIndex(m, leading.getKey().getFieldName(), this.reverseIndexTableName, true, leading.getKey(), typeFilter));
			}
			for (Map.Entry<RangeCalculator.MapKey, Set<Range>> equals : indexRanges.entrySet()) {
				Map<RangeCalculator.MapKey, Set<Range>> m = new HashMap<RangeCalculator.MapKey, Set<Range>>();
				m.put(equals.getKey(), equals.getValue());
				if (RangeCalculator.log.isDebugEnabled())
					RangeCalculator.log.debug(("Ranges for Global Index query: " + (m.toString())));

				this.globalIndexResults.putAll(queryGlobalIndex(m, equals.getKey().getFieldName(), this.indexTableName, false, equals.getKey(), typeFilter));
			}
		} catch (TableNotFoundException e) {
			RangeCalculator.log.error("index table not found", e);
			throw new RuntimeException(" index table not found", e);
		}
		if (RangeCalculator.log.isDebugEnabled())
			RangeCalculator.log.debug(("Ranges from Global Index query: " + (globalIndexResults.toString())));

		RangeCalculator.EvaluationContext ctx = new RangeCalculator.EvaluationContext();
		this.getAST().childrenAccept(this, ctx);
		if ((ctx.lastRange.getRanges().size()) == 0) {
			RangeCalculator.log.debug("No resulting range set");
		}else {
			if (RangeCalculator.log.isDebugEnabled())
				RangeCalculator.log.debug(("Setting range results to: " + (ctx.lastRange.getRanges().toString())));

			this.result = ctx.lastRange.getRanges();
		}
	}

	public Set<Range> getResult() {
		return result;
	}

	public Multimap<String, String> getIndexEntries() {
		return indexEntries;
	}

	public Map<String, String> getIndexValues() {
		return indexValues;
	}

	public Map<String, Long> getTermCardinalities() {
		return termCardinalities;
	}

	protected Map<RangeCalculator.MapKey, RangeCalculator.TermRange> queryGlobalIndex(Map<RangeCalculator.MapKey, Set<Range>> indexRanges, String specificFieldName, String tableName, boolean isReverse, RangeCalculator.MapKey override, Set<String> typeFilter) throws TableNotFoundException {
		Map<RangeCalculator.MapKey, RangeCalculator.TermRange> results = new HashMap<RangeCalculator.MapKey, RangeCalculator.TermRange>();
		Set<Range> rangeSuperSet = new HashSet<Range>();
		for (Map.Entry<RangeCalculator.MapKey, Set<Range>> entry : indexRanges.entrySet()) {
			rangeSuperSet.addAll(entry.getValue());
			RangeCalculator.TermRange tr = new RangeCalculator.TermRange(entry.getKey().getFieldName(), entry.getKey().getFieldValue());
			if (null == override)
				results.put(entry.getKey(), tr);
			else
				results.put(override, tr);

		}
		if (RangeCalculator.log.isDebugEnabled())
			RangeCalculator.log.debug(((((("Querying global index table: " + tableName) + ", range: ") + (rangeSuperSet.toString())) + " colf: ") + specificFieldName));

		BatchScanner bs = this.c.createBatchScanner(tableName, this.auths, this.queryThreads);
		bs.setRanges(rangeSuperSet);
		if (null != specificFieldName) {
			bs.fetchColumnFamily(new Text(specificFieldName));
		}
		for (Map.Entry<Key, Value> entry : bs) {
			if (RangeCalculator.log.isDebugEnabled()) {
				RangeCalculator.log.debug(("Index entry: " + (entry.getKey().toString())));
			}
			String fieldValue = null;
			if (!isReverse) {
				fieldValue = entry.getKey().getRow().toString();
			}else {
				StringBuilder buf = new StringBuilder(entry.getKey().getRow().toString());
				fieldValue = buf.reverse().toString();
			}
			String fieldName = entry.getKey().getColumnFamily().toString();
			String colq = entry.getKey().getColumnQualifier().toString();
			int separator = colq.indexOf(EvaluatingIterator.NULL_BYTE_STRING);
			String shardId = null;
			String datatype = null;
			if (separator != (-1)) {
				shardId = colq.substring(0, separator);
				datatype = colq.substring((separator + 1));
			}else {
				shardId = colq;
			}
			if (((null != datatype) && (null != typeFilter)) && (!(typeFilter.contains(datatype))))
				continue;

			Uid.List uidList = null;
			try {
				uidList = parseFrom(entry.getValue().get());
			} catch (InvalidProtocolBufferException e) {
			}
			long count = 0;
			Long storedCount = termCardinalities.get(fieldName);
			if ((null == storedCount) || (0 == storedCount)) {
				count = uidList.getCOUNT();
			}else {
				count = (uidList.getCOUNT()) + storedCount;
			}
			termCardinalities.put(fieldName, count);
			this.indexEntries.put(fieldName, fieldValue);
			if (null == override)
				this.indexValues.put(fieldValue, fieldValue);
			else
				this.indexValues.put(fieldValue, override.getOriginalQueryValue());

			Text shard = new Text(shardId);
			if (uidList.getIGNORE()) {
				if (null == override)
					results.get(new RangeCalculator.MapKey(fieldName, fieldValue)).add(new Range(shard));
				else
					results.get(override).add(new Range(shard));

			}else {
				for (String uuid : uidList.getUIDList()) {
					Text cf = new Text(datatype);
					TextUtil.textAppend(cf, uuid);
					Key startKey = new Key(shard, cf);
					Key endKey = new Key(shard, new Text(((cf.toString()) + (EvaluatingIterator.NULL_BYTE_STRING))));
					Range eventRange = new Range(startKey, true, endKey, false);
					if (null == override)
						results.get(new RangeCalculator.MapKey(fieldName, fieldValue)).add(eventRange);
					else
						results.get(override).add(eventRange);

				}
			}
		}
		bs.close();
		return results;
	}

	@Override
	public Object visit(ASTOrNode node, Object data) {
		boolean previouslyInOrContext = false;
		RangeCalculator.EvaluationContext ctx = null;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			ctx = ((RangeCalculator.EvaluationContext) (data));
			previouslyInOrContext = ctx.inOrContext;
		}else {
			ctx = new RangeCalculator.EvaluationContext();
		}
		ctx.inOrContext = true;
		node.jjtGetChild(0).jjtAccept(this, ctx);
		Long leftCardinality = this.termCardinalities.get(ctx.lastProcessedTerm);
		if (null == leftCardinality)
			leftCardinality = 0L;

		RangeCalculator.TermRange leftRange = ctx.lastRange;
		if (RangeCalculator.log.isDebugEnabled())
			RangeCalculator.log.debug(((((("[OR-left] term: " + (ctx.lastProcessedTerm)) + ", cardinality: ") + leftCardinality) + ", ranges: ") + (leftRange.getRanges().size())));

		node.jjtGetChild(1).jjtAccept(this, ctx);
		Long rightCardinality = this.termCardinalities.get(ctx.lastProcessedTerm);
		if (null == rightCardinality)
			rightCardinality = 0L;

		RangeCalculator.TermRange rightRange = ctx.lastRange;
		if (RangeCalculator.log.isDebugEnabled())
			RangeCalculator.log.debug(((((("[OR-right] term: " + (ctx.lastProcessedTerm)) + ", cardinality: ") + rightCardinality) + ", ranges: ") + (rightRange.getRanges().size())));

		if ((null != data) && (!previouslyInOrContext))
			ctx.inOrContext = false;

		Set<Range> ranges = new TreeSet<Range>();
		ranges.addAll(leftRange.getRanges());
		ranges.addAll(rightRange.getRanges());
		Set<Text> shardsAdded = new HashSet<Text>();
		Set<Range> returnSet = new HashSet<Range>();
		for (Range r : ranges) {
			if (!(shardsAdded.contains(r.getStartKey().getRow()))) {
				if ((r.getStartKey().getColumnFamily()) == null) {
					shardsAdded.add(r.getStartKey().getRow());
				}
				returnSet.add(r);
			}else {
				RangeCalculator.log.info(((("Skipping event specific range: " + (r.toString())) + " because shard range has already been added: ") + (shardsAdded.contains(r.getStartKey().getRow()))));
			}
		}
		RangeCalculator.TermRange orRange = new RangeCalculator.TermRange("OR_RESULT", "foo");
		orRange.addAll(returnSet);
		if (RangeCalculator.log.isDebugEnabled())
			RangeCalculator.log.debug(("[OR] results: " + (orRange.getRanges().toString())));

		ctx.lastRange = orRange;
		ctx.lastProcessedTerm = "OR_RESULT";
		this.termCardinalities.put("OR_RESULT", (leftCardinality + rightCardinality));
		return null;
	}

	@Override
	public Object visit(ASTAndNode node, Object data) {
		boolean previouslyInAndContext = false;
		RangeCalculator.EvaluationContext ctx = null;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			ctx = ((RangeCalculator.EvaluationContext) (data));
			previouslyInAndContext = ctx.inAndContext;
		}else {
			ctx = new RangeCalculator.EvaluationContext();
		}
		ctx.inAndContext = true;
		node.jjtGetChild(0).jjtAccept(this, ctx);
		String leftTerm = ctx.lastProcessedTerm;
		Long leftCardinality = this.termCardinalities.get(leftTerm);
		if (null == leftCardinality)
			leftCardinality = 0L;

		RangeCalculator.TermRange leftRange = ctx.lastRange;
		if (RangeCalculator.log.isDebugEnabled())
			RangeCalculator.log.debug(((((("[AND-left] term: " + (ctx.lastProcessedTerm)) + ", cardinality: ") + leftCardinality) + ", ranges: ") + (leftRange.getRanges().size())));

		node.jjtGetChild(1).jjtAccept(this, ctx);
		String rightTerm = ctx.lastProcessedTerm;
		Long rightCardinality = this.termCardinalities.get(rightTerm);
		if (null == rightCardinality)
			rightCardinality = 0L;

		RangeCalculator.TermRange rightRange = ctx.lastRange;
		if (RangeCalculator.log.isDebugEnabled())
			RangeCalculator.log.debug(((((("[AND-right] term: " + (ctx.lastProcessedTerm)) + ", cardinality: ") + rightCardinality) + ", ranges: ") + (rightRange.getRanges().size())));

		if ((null != data) && (!previouslyInAndContext))
			ctx.inAndContext = false;

		long card = 0L;
		RangeCalculator.TermRange andRange = new RangeCalculator.TermRange("AND_RESULT", "foo");
		if (((leftCardinality > 0) && (leftCardinality <= rightCardinality)) || (rightCardinality == 0)) {
			card = leftCardinality;
			andRange.addAll(leftRange.getRanges());
		}else
			if (((rightCardinality > 0) && (rightCardinality <= leftCardinality)) || (leftCardinality == 0)) {
				card = rightCardinality;
				andRange.addAll(rightRange.getRanges());
			}

		if (RangeCalculator.log.isDebugEnabled())
			RangeCalculator.log.debug(("[AND] results: " + (andRange.getRanges().toString())));

		ctx.lastRange = andRange;
		ctx.lastProcessedTerm = "AND_RESULT";
		this.termCardinalities.put("AND_RESULT", card);
		return null;
	}

	@Override
	public Object visit(ASTEQNode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = false;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		termsCopy.put(fieldName.toString(), term);
		String termValue = null;
		if ((((String) (term.getValue())).startsWith("'")) && (((String) (term.getValue())).endsWith("'")))
			termValue = ((String) (term.getValue())).substring(1, ((((String) (term.getValue())).length()) - 1));
		else
			termValue = ((String) (term.getValue()));

		RangeCalculator.TermRange ranges = null;
		for (RangeCalculator.MapKey key : this.originalQueryValues.get(termValue)) {
			if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
				ranges = this.globalIndexResults.get(key);
				if (RangeCalculator.log.isDebugEnabled())
					RangeCalculator.log.debug(((("Results for cached index ranges for key: " + key) + " are ") + ranges));

			}
		}
		if (null == ranges)
			ranges = new RangeCalculator.TermRange(fieldName.toString(), ((String) (term.getValue())));

		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = ranges;
			ctx.lastProcessedTerm = fieldName.toString();
		}
		return null;
	}

	@Override
	public Object visit(ASTNENode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = true;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		if (negated)
			negatedTerms.add(fieldName.toString());

		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		termsCopy.put(fieldName.toString(), term);
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = new RangeCalculator.TermRange(fieldName.toString(), term.getValue());
			ctx.lastProcessedTerm = fieldName.toString();
			termCardinalities.put(fieldName.toString(), 0L);
		}
		return null;
	}

	@Override
	public Object visit(ASTLTNode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = false;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		termsCopy.put(fieldName.toString(), term);
		String termValue = null;
		if ((((String) (term.getValue())).startsWith("'")) && (((String) (term.getValue())).endsWith("'")))
			termValue = ((String) (term.getValue())).substring(1, ((((String) (term.getValue())).length()) - 1));
		else
			termValue = ((String) (term.getValue()));

		RangeCalculator.TermRange ranges = null;
		for (RangeCalculator.MapKey key : this.originalQueryValues.get(termValue)) {
			if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
				ranges = this.globalIndexResults.get(key);
				if (RangeCalculator.log.isDebugEnabled())
					RangeCalculator.log.debug(((("Results for cached index ranges for key: " + key) + " are ") + ranges));

			}
		}
		if (null == ranges)
			ranges = new RangeCalculator.TermRange(fieldName.toString(), ((String) (term.getValue())));

		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = ranges;
			ctx.lastProcessedTerm = fieldName.toString();
		}
		return null;
	}

	@Override
	public Object visit(ASTGTNode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = false;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		termsCopy.put(fieldName.toString(), term);
		String termValue = null;
		if ((((String) (term.getValue())).startsWith("'")) && (((String) (term.getValue())).endsWith("'")))
			termValue = ((String) (term.getValue())).substring(1, ((((String) (term.getValue())).length()) - 1));
		else
			termValue = ((String) (term.getValue()));

		RangeCalculator.TermRange ranges = null;
		for (RangeCalculator.MapKey key : this.originalQueryValues.get(termValue)) {
			if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
				ranges = this.globalIndexResults.get(key);
				if (RangeCalculator.log.isDebugEnabled())
					RangeCalculator.log.debug(((("Results for cached index ranges for key: " + key) + " are ") + ranges));

			}
		}
		if (null == ranges)
			ranges = new RangeCalculator.TermRange(fieldName.toString(), ((String) (term.getValue())));

		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = ranges;
			ctx.lastProcessedTerm = fieldName.toString();
		}
		return null;
	}

	@Override
	public Object visit(ASTLENode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = false;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		termsCopy.put(fieldName.toString(), term);
		String termValue = null;
		if ((((String) (term.getValue())).startsWith("'")) && (((String) (term.getValue())).endsWith("'")))
			termValue = ((String) (term.getValue())).substring(1, ((((String) (term.getValue())).length()) - 1));
		else
			termValue = ((String) (term.getValue()));

		RangeCalculator.TermRange ranges = null;
		for (RangeCalculator.MapKey key : this.originalQueryValues.get(termValue)) {
			if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
				ranges = this.globalIndexResults.get(key);
				if (RangeCalculator.log.isDebugEnabled())
					RangeCalculator.log.debug(((("Results for cached index ranges for key: " + key) + " are ") + ranges));

			}
		}
		if (null == ranges)
			ranges = new RangeCalculator.TermRange(fieldName.toString(), ((String) (term.getValue())));

		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = ranges;
			ctx.lastProcessedTerm = fieldName.toString();
		}
		return null;
	}

	@Override
	public Object visit(ASTGENode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = false;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		termsCopy.put(fieldName.toString(), term);
		String termValue = null;
		if ((((String) (term.getValue())).startsWith("'")) && (((String) (term.getValue())).endsWith("'")))
			termValue = ((String) (term.getValue())).substring(1, ((((String) (term.getValue())).length()) - 1));
		else
			termValue = ((String) (term.getValue()));

		RangeCalculator.TermRange ranges = null;
		for (RangeCalculator.MapKey key : this.originalQueryValues.get(termValue)) {
			if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
				ranges = this.globalIndexResults.get(key);
				if (RangeCalculator.log.isDebugEnabled())
					RangeCalculator.log.debug(((("Results for cached index ranges for key: " + key) + " are ") + ranges));

			}
		}
		if (null == ranges)
			ranges = new RangeCalculator.TermRange(fieldName.toString(), ((String) (term.getValue())));

		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = ranges;
			ctx.lastProcessedTerm = fieldName.toString();
		}
		return null;
	}

	@Override
	public Object visit(ASTERNode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = false;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		termsCopy.put(fieldName.toString(), term);
		String termValue = null;
		if ((((String) (term.getValue())).startsWith("'")) && (((String) (term.getValue())).endsWith("'")))
			termValue = ((String) (term.getValue())).substring(1, ((((String) (term.getValue())).length()) - 1));
		else
			termValue = ((String) (term.getValue()));

		RangeCalculator.TermRange ranges = null;
		for (RangeCalculator.MapKey key : this.originalQueryValues.get(termValue)) {
			if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
				ranges = this.globalIndexResults.get(key);
				if (RangeCalculator.log.isDebugEnabled())
					RangeCalculator.log.debug(((("Results for cached index ranges for key: " + key) + " are ") + ranges));

			}
		}
		if (null == ranges)
			ranges = new RangeCalculator.TermRange(fieldName.toString(), ((String) (term.getValue())));

		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = ranges;
			ctx.lastProcessedTerm = fieldName.toString();
		}
		return null;
	}

	@Override
	public Object visit(ASTNRNode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = true;
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		if (negated)
			negatedTerms.add(fieldName.toString());

		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		termsCopy.put(fieldName.toString(), term);
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = new RangeCalculator.TermRange(fieldName.toString(), term.getValue());
			ctx.lastProcessedTerm = fieldName.toString();
			termCardinalities.put(fieldName.toString(), 0L);
		}
		return null;
	}

	@Override
	public Object visit(ASTNullLiteral node, Object data) {
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = new RangeCalculator.TermRange("null", "null");
			ctx.lastProcessedTerm = "null";
			termCardinalities.put("null", 0L);
		}
		return new QueryParser.LiteralResult(node.image);
	}

	@Override
	public Object visit(ASTTrueNode node, Object data) {
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = new RangeCalculator.TermRange("true", "true");
			ctx.lastProcessedTerm = "true";
			termCardinalities.put("true", 0L);
		}
		return new QueryParser.LiteralResult(node.image);
	}

	@Override
	public Object visit(ASTFalseNode node, Object data) {
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = new RangeCalculator.TermRange("false", "false");
			ctx.lastProcessedTerm = "false";
			termCardinalities.put("false", 0L);
		}
		return new QueryParser.LiteralResult(node.image);
	}

	@Override
	public Object visit(ASTFunctionNode node, Object data) {
		QueryParser.FunctionResult fr = new QueryParser.FunctionResult();
		int argc = (node.jjtGetNumChildren()) - 2;
		for (int i = 0; i < argc; i++) {
			Object result = node.jjtGetChild((i + 2)).jjtAccept(this, data);
			if (result instanceof QueryParser.TermResult) {
				QueryParser.TermResult tr = ((QueryParser.TermResult) (result));
				fr.getTerms().add(tr);
				termsCopy.put(((String) (tr.value)), null);
			}
		}
		if ((null != data) && (data instanceof RangeCalculator.EvaluationContext)) {
			RangeCalculator.EvaluationContext ctx = ((RangeCalculator.EvaluationContext) (data));
			ctx.lastRange = new RangeCalculator.TermRange(node.jjtGetChild(0).image, node.jjtGetChild(1).image);
			ctx.lastProcessedTerm = node.jjtGetChild(0).image;
			termCardinalities.put(node.jjtGetChild(0).image, 0L);
		}
		return fr;
	}
}

