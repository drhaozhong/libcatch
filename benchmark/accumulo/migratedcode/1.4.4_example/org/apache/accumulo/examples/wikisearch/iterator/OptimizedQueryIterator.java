package org.apache.accumulo.examples.wikisearch.iterator;


import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class OptimizedQueryIterator implements OptionDescriber , SortedKeyValueIterator<Key, Value> {
	private static Logger log = Logger.getLogger(OptimizedQueryIterator.class);

	private EvaluatingIterator event = null;

	private SortedKeyValueIterator<Key, Value> index = null;

	private Key key = null;

	private Value value = null;

	private boolean eventSpecificRange = false;

	public OptionDescriber.IteratorOptions describeOptions() {
		Map<String, String> options = new HashMap<String, String>();
		options.put(EvaluatingIterator.QUERY_OPTION, "full query expression");
		options.put(BooleanLogicIterator.FIELD_INDEX_QUERY, "modified query for the field index query portion");
		options.put(ReadAheadIterator.QUEUE_SIZE, "parallel queue size");
		options.put(ReadAheadIterator.TIMEOUT, "parallel iterator timeout");
		return new OptionDescriber.IteratorOptions(getClass().getSimpleName(), "evaluates event objects against an expression using the field index", options, null);
	}

	public boolean validateOptions(Map<String, String> options) {
		if ((options.containsKey(EvaluatingIterator.QUERY_OPTION)) && (options.containsKey(BooleanLogicIterator.FIELD_INDEX_QUERY))) {
			return true;
		}
		return false;
	}

	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		if (!(validateOptions(options))) {
			throw new IllegalArgumentException("Invalid options");
		}
		event = new EvaluatingIterator();
		event.init(source.deepCopy(env), options, env);
		if ((options.containsKey(ReadAheadIterator.QUEUE_SIZE)) && (options.containsKey(ReadAheadIterator.TIMEOUT))) {
			BooleanLogicIterator bli = new BooleanLogicIterator();
			bli.init(source, options, env);
			index = new ReadAheadIterator();
			index.init(bli, options, env);
		}else {
			index = new BooleanLogicIterator();
			index.init(source, options, env);
		}
	}

	public OptimizedQueryIterator() {
	}

	public OptimizedQueryIterator(OptimizedQueryIterator other, IteratorEnvironment env) {
		this.event = other.event;
		this.index = other.index;
	}

	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new OptimizedQueryIterator(this, env);
	}

	public Key getTopKey() {
		if (OptimizedQueryIterator.log.isDebugEnabled()) {
			OptimizedQueryIterator.log.debug(("getTopKey: " + (key)));
		}
		return key;
	}

	public Value getTopValue() {
		if (OptimizedQueryIterator.log.isDebugEnabled()) {
			OptimizedQueryIterator.log.debug(("getTopValue: " + (value)));
		}
		return value;
	}

	public boolean hasTop() {
		if (OptimizedQueryIterator.log.isDebugEnabled()) {
			OptimizedQueryIterator.log.debug(("hasTop: returned: " + ((key) != null)));
		}
		return (key) != null;
	}

	public void next() throws IOException {
		if (OptimizedQueryIterator.log.isDebugEnabled()) {
			OptimizedQueryIterator.log.debug("next");
		}
		if ((key) != null) {
			key = null;
			value = null;
		}
		if (eventSpecificRange) {
			event.next();
			if (event.hasTop()) {
				key = event.getTopKey();
				value = event.getTopValue();
			}
		}else {
			do {
				index.next();
				if (index.hasTop()) {
					Key eventKey = index.getTopKey();
					Key endKey = eventKey.followingKey(PartialKey.ROW_COLFAM);
					Key startKey = new Key(eventKey.getRow(), eventKey.getColumnFamily());
					Range eventRange = new Range(startKey, endKey);
					HashSet<ByteSequence> cf = new HashSet<ByteSequence>();
					cf.add(eventKey.getColumnFamilyData());
					event.seek(eventRange, cf, true);
					if (event.hasTop()) {
						key = event.getTopKey();
						value = event.getTopValue();
					}
				}
			} while (((key) == null) && (index.hasTop()) );
		}
		if (!((((key) == null) && ((value) == null)) || (((key) != null) && ((value) != null)))) {
			OptimizedQueryIterator.log.warn(("Key: " + ((key) == null ? "null" : key.toString())));
			OptimizedQueryIterator.log.warn(("Value: " + ((value) == null ? "null" : value.toString())));
			throw new IOException("Return values are inconsistent");
		}
	}

	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		if (OptimizedQueryIterator.log.isDebugEnabled()) {
			OptimizedQueryIterator.log.debug(("seek, range:" + range));
		}
		if (((null != (range.getEndKey())) && ((range.getEndKey().getColumnFamily()) != null)) && ((range.getEndKey().getColumnFamily().getLength()) != 0)) {
			if (OptimizedQueryIterator.log.isDebugEnabled()) {
				OptimizedQueryIterator.log.debug("Jumping straight to the event");
			}
			eventSpecificRange = true;
			event.seek(range, columnFamilies, inclusive);
			if (event.hasTop()) {
				key = event.getTopKey();
				value = event.getTopValue();
			}
		}else {
			if (OptimizedQueryIterator.log.isDebugEnabled()) {
				OptimizedQueryIterator.log.debug("Using BooleanLogicIteratorJexl");
			}
			index.seek(range, columnFamilies, inclusive);
			if (index.hasTop()) {
				Key eventKey = index.getTopKey();
				Range eventRange = new Range(eventKey.getRow());
				HashSet<ByteSequence> cf = new HashSet<ByteSequence>();
				cf.add(eventKey.getColumnFamilyData());
				event.seek(eventRange, cf, true);
				if (event.hasTop()) {
					key = event.getTopKey();
					value = event.getTopValue();
				}else {
					next();
				}
			}
		}
	}
}

