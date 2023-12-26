package org.apache.accumulo.examples.wikisearch.logic;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaMapper;
import org.apache.accumulo.examples.wikisearch.iterator.BooleanLogicIterator;
import org.apache.accumulo.examples.wikisearch.iterator.EvaluatingIterator;
import org.apache.accumulo.examples.wikisearch.iterator.OptimizedQueryIterator;
import org.apache.accumulo.examples.wikisearch.iterator.ReadAheadIterator;
import org.apache.accumulo.examples.wikisearch.parser.EventFields;
import org.apache.accumulo.examples.wikisearch.parser.FieldIndexQueryReWriter;
import org.apache.accumulo.examples.wikisearch.parser.JexlOperatorConstants;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser;
import org.apache.accumulo.examples.wikisearch.parser.RangeCalculator;
import org.apache.accumulo.examples.wikisearch.sample.Document;
import org.apache.accumulo.examples.wikisearch.sample.Field;
import org.apache.accumulo.examples.wikisearch.sample.Results;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public abstract class AbstractQueryLogic {
	protected static Logger log = Logger.getLogger(AbstractQueryLogic.class);

	public static final String DATATYPE_FILTER_SET = "datatype.filter.set";

	private static class DoNotPerformOptimizedQueryException extends Exception {
		private static final long serialVersionUID = 1L;
	}

	public static abstract class IndexRanges {
		private Map<String, String> indexValuesToOriginalValues = null;

		private Multimap<String, String> fieldNamesAndValues = HashMultimap.create();

		private Map<String, Long> termCardinality = new HashMap<String, Long>();

		protected Map<String, TreeSet<Range>> ranges = new HashMap<String, TreeSet<Range>>();

		public Multimap<String, String> getFieldNamesAndValues() {
			return fieldNamesAndValues;
		}

		public void setFieldNamesAndValues(Multimap<String, String> fieldNamesAndValues) {
			this.fieldNamesAndValues = fieldNamesAndValues;
		}

		public final Map<String, Long> getTermCardinality() {
			return termCardinality;
		}

		public Map<String, String> getIndexValuesToOriginalValues() {
			return indexValuesToOriginalValues;
		}

		public void setIndexValuesToOriginalValues(Map<String, String> indexValuesToOriginalValues) {
			this.indexValuesToOriginalValues = indexValuesToOriginalValues;
		}

		public abstract void add(String term, Range r);

		public abstract Set<Range> getRanges();
	}

	public static class UnionIndexRanges extends AbstractQueryLogic.IndexRanges {
		public static String DEFAULT_KEY = "default";

		public UnionIndexRanges() {
			this.ranges.put(AbstractQueryLogic.UnionIndexRanges.DEFAULT_KEY, new TreeSet<Range>());
		}

		public Set<Range> getRanges() {
			Set<Text> shardsAdded = new HashSet<Text>();
			Set<Range> returnSet = new HashSet<Range>();
			for (Range r : ranges.get(AbstractQueryLogic.UnionIndexRanges.DEFAULT_KEY)) {
				if (!(shardsAdded.contains(r.getStartKey().getRow()))) {
					if ((r.getStartKey().getColumnFamily()) == null) {
						shardsAdded.add(r.getStartKey().getRow());
					}
					returnSet.add(r);
				}else {
					AbstractQueryLogic.log.info(((("Skipping event specific range: " + (r.toString())) + " because range has already been added: ") + (shardsAdded.contains(r.getStartKey().getRow()))));
				}
			}
			return returnSet;
		}

		public void add(String term, Range r) {
			ranges.get(AbstractQueryLogic.UnionIndexRanges.DEFAULT_KEY).add(r);
		}
	}

	private String metadataTableName;

	private String indexTableName;

	private String reverseIndexTableName;

	private String tableName;

	private int queryThreads = 8;

	private String readAheadQueueSize;

	private String readAheadTimeOut;

	private boolean useReadAheadIterator;

	private org.apache.accumulo.proxy.thrift.Key kryo = new org.apache.accumulo.proxy.thrift.Key();

	private EventFields eventFields = new EventFields();

	private List<String> unevaluatedFields = null;

	private Map<Class<? extends Formatter>, Formatter> normalizerCacheMap = new HashMap<Class<? extends Formatter>, Formatter>();

	private static final String NULL_BYTE = "\u0000";

	public AbstractQueryLogic() {
		super();
		EventFields.initializeKryo(this.kryo);
	}

	protected Map<String, Multimap<String, Class<? extends Formatter>>> findIndexedTerms(Connector c, Authorizations auths, Set<String> queryLiterals, Set<String> datatypes) throws IllegalAccessException, InstantiationException, TableNotFoundException {
		Map<String, Multimap<String, Class<? extends org.apache.accumulo.core.util.format.Formatter>>> results = new HashMap<String, Multimap<String, Class<? extends org.apache.accumulo.core.util.format.Formatter>>>();
		for (String literal : queryLiterals) {
			if (AbstractQueryLogic.log.isDebugEnabled())
				AbstractQueryLogic.log.debug(((("Querying " + (this.getMetadataTableName())) + " table for ") + literal));

			Range range = new Range(literal.toUpperCase());
			Scanner scanner = c.createScanner(this.getMetadataTableName(), auths);
			scanner.setRange(range);
			scanner.fetchColumnFamily(new Text(WikipediaMapper.METADATA_INDEX_COLUMN_FAMILY));
			for (Map.Entry<Key, Value> entry : scanner) {
				if (!(results.containsKey(literal))) {
					Multimap<String, Class<? extends Formatter>> m = HashMultimap.create();
				}
				String colq = entry.getKey().getColumnQualifier().toString();
				if ((null != colq) && (colq.contains("\u0000"))) {
					int idx = colq.indexOf("\u0000");
					if (idx != (-1)) {
						String type = colq.substring(0, idx);
						if ((null != datatypes) && (!(datatypes.contains(type))))
							continue;

						try {
							@SuppressWarnings("unchecked")
							Class<? extends Formatter> clazz = ((Class<? extends Formatter>) (Class.forName(colq.substring((idx + 1)))));
							if (!(this.normalizerCacheMap.containsKey(clazz)))
								this.normalizerCacheMap.put(clazz, clazz.newInstance());

						} catch (ClassNotFoundException e) {
							AbstractQueryLogic.log.error(("Unable to find normalizer on class path: " + (colq.substring((idx + 1)))), e);
						}
					}else {
						AbstractQueryLogic.log.warn(("EventMetadata entry did not contain NULL byte: " + (entry.getKey().toString())));
					}
				}else {
					AbstractQueryLogic.log.warn(("ColumnQualifier null in EventMetadata for key: " + (entry.getKey().toString())));
				}
			}
		}
		if (AbstractQueryLogic.log.isDebugEnabled())
			AbstractQueryLogic.log.debug(("METADATA RESULTS: " + (results.toString())));

		return results;
	}

	protected abstract AbstractQueryLogic.IndexRanges getTermIndexInformation(Connector c, Authorizations auths, String value, Set<String> datatypes) throws TableNotFoundException;

	protected abstract RangeCalculator getTermIndexInformation(Connector c, Authorizations auths, Multimap<String, Formatter> indexedTerms, Multimap<String, QueryParser.QueryTerm> terms, String indexTableName, String reverseIndexTableName, String queryString, int queryThreads, Set<String> datatypes) throws TableNotFoundException, ParseException;

	protected abstract Collection<Range> getFullScanRange(Date begin, Date end, Multimap<String, QueryParser.QueryTerm> terms);

	public String getMetadataTableName() {
		return metadataTableName;
	}

	public String getIndexTableName() {
		return indexTableName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setMetadataTableName(String metadataTableName) {
		this.metadataTableName = metadataTableName;
	}

	public void setIndexTableName(String indexTableName) {
		this.indexTableName = indexTableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getQueryThreads() {
		return queryThreads;
	}

	public void setQueryThreads(int queryThreads) {
		this.queryThreads = queryThreads;
	}

	public String getReadAheadQueueSize() {
		return readAheadQueueSize;
	}

	public String getReadAheadTimeOut() {
		return readAheadTimeOut;
	}

	public boolean isUseReadAheadIterator() {
		return useReadAheadIterator;
	}

	public void setReadAheadQueueSize(String readAheadQueueSize) {
		this.readAheadQueueSize = readAheadQueueSize;
	}

	public void setReadAheadTimeOut(String readAheadTimeOut) {
		this.readAheadTimeOut = readAheadTimeOut;
	}

	public void setUseReadAheadIterator(boolean useReadAheadIterator) {
		this.useReadAheadIterator = useReadAheadIterator;
	}

	public String getReverseIndexTableName() {
		return reverseIndexTableName;
	}

	public void setReverseIndexTableName(String reverseIndexTableName) {
		this.reverseIndexTableName = reverseIndexTableName;
	}

	public List<String> getUnevaluatedFields() {
		return unevaluatedFields;
	}

	public void setUnevaluatedFields(List<String> unevaluatedFields) {
		this.unevaluatedFields = unevaluatedFields;
	}

	public void setUnevaluatedFields(String unevaluatedFieldList) {
		this.unevaluatedFields = new ArrayList<String>();
		for (String field : unevaluatedFieldList.split(","))
			this.unevaluatedFields.add(field);

	}

	public Document createDocument(Key key, Value value) {
		Document doc = new Document();
		this.eventFields.clear();
		ByteBuffer buf = ByteBuffer.wrap(value.get());
		String row = key.getRow().toString();
		String colf = key.getColumnFamily().toString();
		int idx = colf.indexOf(AbstractQueryLogic.NULL_BYTE);
		String type = colf.substring(0, idx);
		String id = colf.substring((idx + 1));
		doc.setId(id);
		for (Map.Entry<String, Collection<EventFields.FieldValue>> entry : this.eventFields.asMap().entrySet()) {
			for (EventFields.FieldValue fv : entry.getValue()) {
				Field val = new Field();
				val.setFieldName(entry.getKey());
				val.setFieldValue(new String(fv.getValue(), Charset.forName("UTF-8")));
				doc.getFields().add(val);
			}
		}
		Field docPointer = new Field();
		docPointer.setFieldName("DOCUMENT");
		docPointer.setFieldValue(((((("DOCUMENT:" + row) + "/") + type) + "/") + id));
		doc.getFields().add(docPointer);
		return doc;
	}

	public String getResultsKey(Map.Entry<Key, Value> key) {
		return key.getKey().getColumnFamily().toString();
	}

	public Results runQuery(Connector connector, List<String> authorizations, String query, Date beginDate, Date endDate, Set<String> types) {
		if (StringUtils.isEmpty(query)) {
			throw new IllegalArgumentException(("NULL QueryNode reference passed to " + (this.getClass().getSimpleName())));
		}
		Set<Range> ranges = new HashSet<Range>();
		Set<String> typeFilter = types;
		String[] array = authorizations.toArray(new String[0]);
		Authorizations auths = new Authorizations(array);
		Results results = new Results();
		String queryString = query;
		StopWatch abstractQueryLogic = new StopWatch();
		StopWatch optimizedQuery = new StopWatch();
		StopWatch queryGlobalIndex = new StopWatch();
		StopWatch optimizedEventQuery = new StopWatch();
		StopWatch fullScanQuery = new StopWatch();
		StopWatch processResults = new StopWatch();
		abstractQueryLogic.start();
		StopWatch parseQuery = new StopWatch();
		parseQuery.start();
		QueryParser parser;
		try {
			if (AbstractQueryLogic.log.isDebugEnabled()) {
				AbstractQueryLogic.log.debug("ShardQueryLogic calling QueryParser.execute");
			}
			parser = new QueryParser();
			parser.execute(queryString);
		} catch (ParseException e1) {
			throw new IllegalArgumentException("Error parsing query", e1);
		}
		int hash = parser.getHashValue();
		parseQuery.stop();
		if (AbstractQueryLogic.log.isDebugEnabled()) {
			AbstractQueryLogic.log.debug(((hash + " Query: ") + queryString));
		}
		Set<String> fields = new HashSet<String>();
		for (String f : parser.getQueryIdentifiers()) {
			fields.add(f);
		}
		if (AbstractQueryLogic.log.isDebugEnabled()) {
			AbstractQueryLogic.log.debug(("getQueryIdentifiers: " + (parser.getQueryIdentifiers().toString())));
		}
		fields.removeAll(parser.getNegatedTermsForOptimizer());
		if (AbstractQueryLogic.log.isDebugEnabled()) {
			AbstractQueryLogic.log.debug(("getQueryIdentifiers: " + (parser.getQueryIdentifiers().toString())));
		}
		Multimap<String, QueryParser.QueryTerm> terms = parser.getQueryTerms();
		StopWatch queryMetadata = new StopWatch();
		queryMetadata.start();
		Map<String, Multimap<String, Class<? extends org.apache.accumulo.core.util.format.Formatter>>> metadataResults;
		try {
			metadataResults = findIndexedTerms(connector, auths, fields, typeFilter);
		} catch (Exception e1) {
			throw new RuntimeException("Error in metadata lookup", e1);
		}
		Multimap<String, Formatter> indexedTerms = HashMultimap.create();
		for (Map.Entry<String, Multimap<String, Class<? extends org.apache.accumulo.core.util.format.Formatter>>> entry : metadataResults.entrySet()) {
			for (Class<? extends org.apache.accumulo.core.util.format.Formatter> clazz : entry.getValue().values()) {
			}
		}
		queryMetadata.stop();
		if (AbstractQueryLogic.log.isDebugEnabled()) {
			AbstractQueryLogic.log.debug(((hash + " Indexed Terms: ") + (indexedTerms.toString())));
		}
		Set<String> orTerms = parser.getOrTermsForOptimizer();
		ArrayList<String> unevaluatedExpressions = new ArrayList<String>();
		boolean unsupportedOperatorSpecified = false;
		for (Map.Entry<String, QueryParser.QueryTerm> entry : terms.entries()) {
			if (null == (entry.getValue())) {
				continue;
			}
			if ((null != (this.unevaluatedFields)) && (this.unevaluatedFields.contains(entry.getKey().trim()))) {
				unevaluatedExpressions.add((((((entry.getKey().trim()) + " ") + (entry.getValue().getOperator())) + " ") + (entry.getValue().getValue())));
			}
			int operator = JexlOperatorConstants.getJJTNodeType(entry.getValue().getOperator());
			if (!(((((((operator == (ParserTreeConstants.JJTEQNODE)) || (operator == (ParserTreeConstants.JJTNENODE))) || (operator == (ParserTreeConstants.JJTLENODE))) || (operator == (ParserTreeConstants.JJTLTNODE))) || (operator == (ParserTreeConstants.JJTGENODE))) || (operator == (ParserTreeConstants.JJTGTNODE))) || (operator == (ParserTreeConstants.JJTERNODE)))) {
				unsupportedOperatorSpecified = true;
				break;
			}
		}
		if (null != unevaluatedExpressions)
			unevaluatedExpressions.trimToSize();

		if (AbstractQueryLogic.log.isDebugEnabled()) {
			AbstractQueryLogic.log.debug(((((((((hash + " unsupportedOperators: ") + unsupportedOperatorSpecified) + " indexedTerms: ") + (indexedTerms.toString())) + " orTerms: ") + (orTerms.toString())) + " unevaluatedExpressions: ") + (unevaluatedExpressions.toString())));
		}
		boolean optimizationSucceeded = false;
		boolean orsAllIndexed = false;
		if (orTerms.isEmpty()) {
			orsAllIndexed = false;
		}else {
			orsAllIndexed = indexedTerms.keySet().containsAll(orTerms);
		}
		if (AbstractQueryLogic.log.isDebugEnabled()) {
			AbstractQueryLogic.log.debug("All or terms are indexed");
		}
		if ((!unsupportedOperatorSpecified) && (((((null == orTerms) || (orTerms.isEmpty())) && ((indexedTerms.size()) > 0)) || (((fields.size()) > 0) && ((indexedTerms.size()) == (fields.size())))) || orsAllIndexed)) {
			optimizedQuery.start();
			queryGlobalIndex.start();
			AbstractQueryLogic.IndexRanges termIndexInfo;
			try {
				if (fields.isEmpty()) {
					termIndexInfo = this.getTermIndexInformation(connector, auths, queryString, typeFilter);
					if ((null != termIndexInfo) && (termIndexInfo.getRanges().isEmpty())) {
						throw new AbstractQueryLogic.DoNotPerformOptimizedQueryException();
					}
					if (termIndexInfo instanceof AbstractQueryLogic.UnionIndexRanges) {
						AbstractQueryLogic.UnionIndexRanges union = ((AbstractQueryLogic.UnionIndexRanges) (termIndexInfo));
						StringBuilder buf = new StringBuilder();
						String sep = "";
						for (String fieldName : union.getFieldNamesAndValues().keySet()) {
							buf.append(sep).append(fieldName).append(" == ");
							if (!((queryString.startsWith("'")) && (queryString.endsWith("'")))) {
								buf.append("'").append(queryString).append("'");
							}else {
								buf.append(queryString);
							}
							sep = " or ";
						}
						if (AbstractQueryLogic.log.isDebugEnabled()) {
							AbstractQueryLogic.log.debug(((("Rewrote query for non-fielded single term query: " + queryString) + " to ") + (buf.toString())));
						}
						queryString = buf.toString();
					}else {
						throw new RuntimeException("Unexpected IndexRanges implementation");
					}
				}else {
					RangeCalculator calc = this.getTermIndexInformation(connector, auths, indexedTerms, terms, this.getIndexTableName(), this.getReverseIndexTableName(), queryString, this.queryThreads, typeFilter);
					if ((null == (calc.getResult())) || (calc.getResult().isEmpty())) {
						throw new AbstractQueryLogic.DoNotPerformOptimizedQueryException();
					}
					termIndexInfo = new AbstractQueryLogic.UnionIndexRanges();
					termIndexInfo.setIndexValuesToOriginalValues(calc.getIndexValues());
					termIndexInfo.setFieldNamesAndValues(calc.getIndexEntries());
					termIndexInfo.getTermCardinality().putAll(calc.getTermCardinalities());
					for (Range r : calc.getResult()) {
						termIndexInfo.add("foo", r);
					}
				}
			} catch (TableNotFoundException e) {
				AbstractQueryLogic.log.error(((this.getIndexTableName()) + "not found"), e);
				throw new RuntimeException(((this.getIndexTableName()) + "not found"), e);
			} catch (ParseException e) {
				throw new RuntimeException(("Error determining ranges for query: " + queryString), e);
			} catch (AbstractQueryLogic.DoNotPerformOptimizedQueryException e) {
				AbstractQueryLogic.log.info("Indexed fields not found in index, performing full scan");
				termIndexInfo = null;
			}
			queryGlobalIndex.stop();
			boolean proceed = false;
			if ((null == termIndexInfo) || ((termIndexInfo.getFieldNamesAndValues().values().size()) == 0)) {
				proceed = false;
			}else
				if (((null != orTerms) && ((orTerms.size()) > 0)) && ((termIndexInfo.getFieldNamesAndValues().values().size()) == (indexedTerms.size()))) {
					proceed = true;
				}else
					if ((termIndexInfo.getFieldNamesAndValues().values().size()) > 0) {
						proceed = true;
					}else
						if (orsAllIndexed) {
							proceed = true;
						}else {
							proceed = false;
						}



			if (AbstractQueryLogic.log.isDebugEnabled()) {
				AbstractQueryLogic.log.debug(("Proceed with optimized query: " + proceed));
				if (null != termIndexInfo)
					AbstractQueryLogic.log.debug(((((("termIndexInfo.getTermsFound().size(): " + (termIndexInfo.getFieldNamesAndValues().values().size())) + " indexedTerms.size: ") + (indexedTerms.size())) + " fields.size: ") + (fields.size())));

			}
			if (proceed) {
				if (AbstractQueryLogic.log.isDebugEnabled()) {
					AbstractQueryLogic.log.debug((hash + " Performing optimized query"));
				}
				ranges = termIndexInfo.getRanges();
				if (AbstractQueryLogic.log.isDebugEnabled()) {
					AbstractQueryLogic.log.info(((((hash + " Ranges: count: ") + (ranges.size())) + ", ") + (ranges.toString())));
				}
				optimizedEventQuery.start();
				BatchScanner bs = null;
				try {
					bs = connector.createBatchScanner(this.getTableName(), auths, queryThreads);
					bs.setRanges(ranges);
					IteratorSetting si = new IteratorSetting(21, "eval", OptimizedQueryIterator.class);
					if (AbstractQueryLogic.log.isDebugEnabled()) {
						AbstractQueryLogic.log.debug(((("Setting scan option: " + (EvaluatingIterator.QUERY_OPTION)) + " to ") + queryString));
					}
					si.addOption(EvaluatingIterator.QUERY_OPTION, queryString);
					StringBuilder buf = new StringBuilder();
					String sep = "";
					for (Map.Entry<String, String> entry : termIndexInfo.getFieldNamesAndValues().entries()) {
						buf.append(sep);
						buf.append(entry.getKey());
						buf.append(":");
						buf.append(termIndexInfo.getIndexValuesToOriginalValues().get(entry.getValue()));
						buf.append(":");
						buf.append(entry.getValue());
						if (sep.equals("")) {
							sep = ";";
						}
					}
					if (AbstractQueryLogic.log.isDebugEnabled()) {
						AbstractQueryLogic.log.debug(((("Setting scan option: " + (FieldIndexQueryReWriter.INDEXED_TERMS_LIST)) + " to ") + (buf.toString())));
					}
					FieldIndexQueryReWriter rewriter = new FieldIndexQueryReWriter();
					String q = "";
					try {
						q = queryString;
						q = rewriter.applyCaseSensitivity(q, true, false);
						Map<String, String> opts = new HashMap<String, String>();
						opts.put(FieldIndexQueryReWriter.INDEXED_TERMS_LIST, buf.toString());
						q = rewriter.removeNonIndexedTermsAndInvalidRanges(q, opts);
						q = rewriter.applyNormalizedTerms(q, opts);
						if (AbstractQueryLogic.log.isDebugEnabled()) {
							AbstractQueryLogic.log.debug(("runServerQuery, FieldIndex Query: " + q));
						}
					} catch (ParseException ex) {
						AbstractQueryLogic.log.error(("Could not parse query, Jexl ParseException: " + ex));
					} catch (Exception ex) {
						AbstractQueryLogic.log.error(("Problem rewriting query, Exception: " + (ex.getMessage())));
					}
					si.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, q);
					sep = "";
					buf.delete(0, buf.length());
					for (Map.Entry<String, Long> entry : termIndexInfo.getTermCardinality().entrySet()) {
						buf.append(sep);
						buf.append(entry.getKey());
						buf.append(":");
						buf.append(entry.getValue());
						sep = ",";
					}
					if (AbstractQueryLogic.log.isDebugEnabled())
						AbstractQueryLogic.log.debug(((("Setting scan option: " + (BooleanLogicIterator.TERM_CARDINALITIES)) + " to ") + (buf.toString())));

					si.addOption(BooleanLogicIterator.TERM_CARDINALITIES, buf.toString());
					if (this.useReadAheadIterator) {
						if (AbstractQueryLogic.log.isDebugEnabled()) {
							AbstractQueryLogic.log.debug(((("Enabling read ahead iterator with queue size: " + (this.readAheadQueueSize)) + " and timeout: ") + (this.readAheadTimeOut)));
						}
						si.addOption(ReadAheadIterator.QUEUE_SIZE, this.readAheadQueueSize);
						si.addOption(ReadAheadIterator.TIMEOUT, this.readAheadTimeOut);
					}
					if (null != unevaluatedExpressions) {
						StringBuilder unevaluatedExpressionList = new StringBuilder();
						String sep2 = "";
						for (String exp : unevaluatedExpressions) {
							unevaluatedExpressionList.append(sep2).append(exp);
							sep2 = ",";
						}
						if (AbstractQueryLogic.log.isDebugEnabled())
							AbstractQueryLogic.log.debug(((("Setting scan option: " + (EvaluatingIterator.UNEVALUTED_EXPRESSIONS)) + " to ") + (unevaluatedExpressionList.toString())));

						si.addOption(EvaluatingIterator.UNEVALUTED_EXPRESSIONS, unevaluatedExpressionList.toString());
					}
					bs.addScanIterator(si);
					processResults.start();
					processResults.suspend();
					long count = 0;
					for (Map.Entry<Key, Value> entry : bs) {
						count++;
						processResults.resume();
						Document d = this.createDocument(entry.getKey(), entry.getValue());
						results.getResults().add(d);
						processResults.suspend();
					}
					AbstractQueryLogic.log.info((count + " matching entries found in optimized query."));
					optimizationSucceeded = true;
					processResults.stop();
				} catch (TableNotFoundException e) {
					AbstractQueryLogic.log.error(((this.getTableName()) + "not found"), e);
					throw new RuntimeException(((this.getIndexTableName()) + "not found"), e);
				} finally {
					if (bs != null) {
						bs.close();
					}
				}
				optimizedEventQuery.stop();
			}
			optimizedQuery.stop();
		}
		if ((!optimizationSucceeded) || ((((null != orTerms) && ((orTerms.size()) > 0)) && ((indexedTerms.size()) != (fields.size()))) && (!orsAllIndexed))) {
			fullScanQuery.start();
			if (AbstractQueryLogic.log.isDebugEnabled()) {
				AbstractQueryLogic.log.debug((hash + " Performing full scan query"));
			}
			BatchScanner bs = null;
			try {
				Collection<Range> r = getFullScanRange(beginDate, endDate, terms);
				ranges.addAll(r);
				if (AbstractQueryLogic.log.isDebugEnabled()) {
					AbstractQueryLogic.log.debug(((((hash + " Ranges: count: ") + (ranges.size())) + ", ") + (ranges.toString())));
				}
				bs = connector.createBatchScanner(this.getTableName(), auths, queryThreads);
				bs.setRanges(ranges);
				IteratorSetting si = new IteratorSetting(22, "eval", EvaluatingIterator.class);
				if (null != typeFilter) {
					StringBuilder buf = new StringBuilder();
					String s = "";
					for (String type : typeFilter) {
						buf.append(s).append(type).append(".*");
						s = "|";
					}
					if (AbstractQueryLogic.log.isDebugEnabled())
						AbstractQueryLogic.log.debug(("Setting colf regex iterator to: " + (buf.toString())));

					IteratorSetting ri = new IteratorSetting(21, "typeFilter", RegExFilter.class);
					RegExFilter.setRegexs(ri, null, buf.toString(), null, null, false);
					bs.addScanIterator(ri);
				}
				if (AbstractQueryLogic.log.isDebugEnabled()) {
					AbstractQueryLogic.log.debug(((("Setting scan option: " + (EvaluatingIterator.QUERY_OPTION)) + " to ") + queryString));
				}
				si.addOption(EvaluatingIterator.QUERY_OPTION, queryString);
				if (null != unevaluatedExpressions) {
					StringBuilder unevaluatedExpressionList = new StringBuilder();
					String sep2 = "";
					for (String exp : unevaluatedExpressions) {
						unevaluatedExpressionList.append(sep2).append(exp);
						sep2 = ",";
					}
					if (AbstractQueryLogic.log.isDebugEnabled())
						AbstractQueryLogic.log.debug(((("Setting scan option: " + (EvaluatingIterator.UNEVALUTED_EXPRESSIONS)) + " to ") + (unevaluatedExpressionList.toString())));

					si.addOption(EvaluatingIterator.UNEVALUTED_EXPRESSIONS, unevaluatedExpressionList.toString());
				}
				bs.addScanIterator(si);
				long count = 0;
				processResults.start();
				processResults.suspend();
				for (Map.Entry<Key, Value> entry : bs) {
					count++;
					processResults.resume();
					Document d = this.createDocument(entry.getKey(), entry.getValue());
					results.getResults().add(d);
					processResults.suspend();
				}
				processResults.stop();
				AbstractQueryLogic.log.info((count + " matching entries found in full scan query."));
			} catch (TableNotFoundException e) {
				AbstractQueryLogic.log.error(((this.getTableName()) + "not found"), e);
			} finally {
				if (bs != null) {
					bs.close();
				}
			}
			fullScanQuery.stop();
		}
		AbstractQueryLogic.log.info(((("AbstractQueryLogic: " + queryString) + " ") + (AbstractQueryLogic.timeString(abstractQueryLogic.getTime()))));
		AbstractQueryLogic.log.info(("  1) parse query " + (AbstractQueryLogic.timeString(parseQuery.getTime()))));
		AbstractQueryLogic.log.info(("  2) query metadata " + (AbstractQueryLogic.timeString(queryMetadata.getTime()))));
		AbstractQueryLogic.log.info(("  3) full scan query " + (AbstractQueryLogic.timeString(fullScanQuery.getTime()))));
		AbstractQueryLogic.log.info(("  3) optimized query " + (AbstractQueryLogic.timeString(optimizedQuery.getTime()))));
		AbstractQueryLogic.log.info(("  1) process results " + (AbstractQueryLogic.timeString(processResults.getTime()))));
		AbstractQueryLogic.log.info(("      1) query global index " + (AbstractQueryLogic.timeString(queryGlobalIndex.getTime()))));
		AbstractQueryLogic.log.info((hash + " Query completed."));
		return results;
	}

	private static String timeString(long millis) {
		return String.format("%4.2f", (millis / 1000.0));
	}
}

