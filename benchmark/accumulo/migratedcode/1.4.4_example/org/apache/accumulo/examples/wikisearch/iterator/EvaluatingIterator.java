package org.apache.accumulo.examples.wikisearch.iterator;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.wikisearch.parser.EventFields;
import org.apache.commons.collections.map.AbstractHashedMap;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.io.Text;


public class EvaluatingIterator extends AbstractEvaluatingIterator {
	public static final String NULL_BYTE_STRING = "\u0000";

	LRUMap visibilityMap = new LRUMap();

	public EvaluatingIterator() {
		super();
	}

	public EvaluatingIterator(AbstractEvaluatingIterator other, IteratorEnvironment env) {
		super(other, env);
	}

	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new EvaluatingIterator(this, env);
	}

	@Override
	public PartialKey getKeyComparator() {
		return PartialKey.ROW_COLFAM;
	}

	@Override
	public Key getReturnKey(Key k) {
		Key r = new Key(k.getRowData().getBackingArray(), k.getColumnFamilyData().getBackingArray(), AbstractEvaluatingIterator.NULL_BYTE, k.getColumnVisibility().getBytes(), k.getTimestamp(), k.isDeleted(), false);
		return r;
	}

	@Override
	public void fillMap(EventFields event, Key key, Value value) {
		String colq = key.getColumnQualifier().toString();
		int idx = colq.indexOf(EvaluatingIterator.NULL_BYTE_STRING);
		String fieldName = colq.substring(0, idx);
		String fieldValue = colq.substring((idx + 1));
		event.put(fieldName, new EventFields.FieldValue(getColumnVisibility(key), fieldValue.getBytes()));
	}

	public ColumnVisibility getColumnVisibility(Key key) {
		ColumnVisibility result = ((ColumnVisibility) (visibilityMap.get(key.getColumnVisibility())));
		if (result != null)
			return result;

		result = new ColumnVisibility(key.getColumnVisibility().getBytes());
		visibilityMap.put(key.getColumnVisibility(), result);
		return result;
	}

	@Override
	public boolean isKeyAccepted(Key key) throws IOException {
		if (key.getColumnFamily().toString().startsWith("fi")) {
			Key copy = new Key(key.getRow(), new Text("fi\u0001"));
			Collection<ByteSequence> columnFamilies = Collections.emptyList();
			this.iterator.seek(new Range(copy, copy), columnFamilies, true);
			if (this.iterator.hasTop())
				return isKeyAccepted(this.iterator.getTopKey());

			return true;
		}
		return true;
	}
}

