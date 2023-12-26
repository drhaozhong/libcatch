package org.apache.accumulo.examples.wikisearch.iterator;


import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.examples.wikisearch.parser.EventFields;
import org.apache.accumulo.examples.wikisearch.parser.QueryEvaluator;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public abstract class AbstractEvaluatingIterator implements OptionDescriber , SortedKeyValueIterator<Key, Value> {
	private static Logger log = Logger.getLogger(AbstractEvaluatingIterator.class);

	protected static final byte[] NULL_BYTE = new byte[0];

	public static final String QUERY_OPTION = "expr";

	public static final String UNEVALUTED_EXPRESSIONS = "unevaluated.expressions";

	private PartialKey comparator = null;

	protected SortedKeyValueIterator<Key, Value> iterator;

	private Key currentKey = new Key();

	private Key returnKey;

	private Value returnValue;

	private String expression;

	private QueryEvaluator evaluator;

	private EventFields event = null;

	private static Key kryo = new org.apache.accumulo.proxy.thrift.Key();

	private Range seekRange = null;

	private Set<String> skipExpressions = null;

	protected AbstractEvaluatingIterator(AbstractEvaluatingIterator other, IteratorEnvironment env) {
		iterator = other.iterator.deepCopy(env);
		event = other.event;
	}

	public AbstractEvaluatingIterator() {
	}

	public abstract PartialKey getKeyComparator();

	public abstract Key getReturnKey(Key k) throws Exception;

	public abstract void fillMap(EventFields event, Key key, Value value) throws Exception;

	public abstract boolean isKeyAccepted(Key key) throws IOException;

	public void reset() {
		event.clear();
	}

	private void aggregateRowColumn(EventFields event) throws IOException {
		currentKey.set(iterator.getTopKey());
		try {
			fillMap(event, iterator.getTopKey(), iterator.getTopValue());
			iterator.next();
			while ((iterator.hasTop()) && (iterator.getTopKey().equals(currentKey, this.comparator))) {
				fillMap(event, iterator.getTopKey(), iterator.getTopValue());
				iterator.next();
			} 
			returnKey = getReturnKey(currentKey);
		} catch (Exception e) {
			throw new IOException("Error aggregating event", e);
		}
	}

	private void findTop() throws IOException {
		do {
			reset();
			if (iterator.hasTop()) {
				while ((iterator.hasTop()) && (!(isKeyAccepted(iterator.getTopKey())))) {
					iterator.next();
				} 
				if (iterator.hasTop()) {
					aggregateRowColumn(event);
					if (((event.size()) > 0) && (this.evaluator.evaluate(event))) {
						if (AbstractEvaluatingIterator.log.isDebugEnabled()) {
							AbstractEvaluatingIterator.log.debug(("Event evaluated to true, key = " + (this.returnKey)));
						}
						byte[] serializedMap = new byte[(this.event.getByteSize()) + ((this.event.size()) * 20)];
						ByteBuffer buf = ByteBuffer.wrap(serializedMap);
						this.returnValue = new Value(Arrays.copyOfRange(serializedMap, 0, buf.position()));
					}else {
						returnKey = null;
						returnValue = null;
					}
				}else {
					if (AbstractEvaluatingIterator.log.isDebugEnabled()) {
						AbstractEvaluatingIterator.log.debug("Iterator no longer has top.");
					}
				}
			}else {
				AbstractEvaluatingIterator.log.debug("Iterator.hasTop() == false");
			}
		} while (((returnValue) == null) && (iterator.hasTop()) );
		if (!((((returnKey) == null) && ((returnValue) == null)) || (((returnKey) != null) && ((returnValue) != null)))) {
			AbstractEvaluatingIterator.log.warn(("Key: " + ((returnKey) == null ? "null" : returnKey.toString())));
			AbstractEvaluatingIterator.log.warn(("Value: " + ((returnValue) == null ? "null" : returnValue.toString())));
			throw new IOException("Return values are inconsistent");
		}
	}

	public Key getTopKey() {
		if ((returnKey) != null) {
			return returnKey;
		}
		return iterator.getTopKey();
	}

	public Value getTopValue() {
		if ((returnValue) != null) {
			return returnValue;
		}
		return iterator.getTopValue();
	}

	public boolean hasTop() {
		return ((returnKey) != null) || (iterator.hasTop());
	}

	public void next() throws IOException {
		if ((returnKey) != null) {
			returnKey = null;
			returnValue = null;
		}else
			if (iterator.hasTop()) {
				iterator.next();
			}

		findTop();
	}

	static Range maximizeStartKeyTimeStamp(Range range) {
		Range seekRange = range;
		if (((range.getStartKey()) != null) && ((range.getStartKey().getTimestamp()) != (Long.MAX_VALUE))) {
			Key seekKey = new Key(seekRange.getStartKey());
			seekKey.setTimestamp(Long.MAX_VALUE);
			seekRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
		}
		return seekRange;
	}

	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		seekRange = AbstractEvaluatingIterator.maximizeStartKeyTimeStamp(range);
		iterator.seek(seekRange, columnFamilies, inclusive);
		findTop();
		if ((range.getStartKey()) != null) {
			while (((hasTop()) && (getTopKey().equals(range.getStartKey(), this.comparator))) && ((getTopKey().getTimestamp()) > (range.getStartKey().getTimestamp()))) {
				next();
			} 
			while ((hasTop()) && (range.beforeStartKey(getTopKey()))) {
				next();
			} 
		}
	}

	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		validateOptions(options);
		this.event = new EventFields();
		this.comparator = getKeyComparator();
		this.iterator = source;
		try {
			if ((null != (this.skipExpressions)) && ((this.skipExpressions.size()) != 0)) {
				for (String skip : this.skipExpressions) {
					String field = skip.substring(0, ((skip.indexOf(" ")) - 1));
					this.expression = this.expression.replaceAll(skip, (field + " == null"));
				}
			}
			this.evaluator = new QueryEvaluator(this.expression);
		} catch (ParseException e) {
			throw new IllegalArgumentException("Failed to parse query", e);
		}
	}

	public OptionDescriber.IteratorOptions describeOptions() {
		Map<String, String> options = new HashMap<String, String>();
		options.put(AbstractEvaluatingIterator.QUERY_OPTION, "query expression");
		options.put(AbstractEvaluatingIterator.UNEVALUTED_EXPRESSIONS, "comma separated list of expressions to skip");
		return new OptionDescriber.IteratorOptions(getClass().getSimpleName(), "evaluates event objects against an expression", options, null);
	}

	public boolean validateOptions(Map<String, String> options) {
		if (!(options.containsKey(AbstractEvaluatingIterator.QUERY_OPTION)))
			return false;
		else
			this.expression = options.get(AbstractEvaluatingIterator.QUERY_OPTION);

		if (options.containsKey(AbstractEvaluatingIterator.UNEVALUTED_EXPRESSIONS)) {
			String expressionList = options.get(AbstractEvaluatingIterator.UNEVALUTED_EXPRESSIONS);
			if ((expressionList != null) && (!(expressionList.trim().equals("")))) {
				this.skipExpressions = new HashSet<String>();
				for (String e : expressionList.split(","))
					this.skipExpressions.add(e);

			}
		}
		return true;
	}

	public String getQueryExpression() {
		return this.expression;
	}
}

