package org.apache.accumulo.examples.wikisearch.iterator;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.examples.wikisearch.util.FieldIndexKeyParser;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class UniqFieldNameValueIterator extends WrappingIterator {
	protected static final Logger log = Logger.getLogger(UniqFieldNameValueIterator.class);

	private SortedKeyValueIterator<Key, Value> source;

	private FieldIndexKeyParser keyParser;

	private Key topKey = null;

	private Value topValue = null;

	private Range overallRange = null;

	private Range currentSubRange;

	private Text fieldName = null;

	private Text fieldValueLowerBound = null;

	private Text fieldValueUpperBound = null;

	private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

	private static final String ONE_BYTE = "\u0001";

	private boolean multiRow = false;

	private boolean seekInclusive = false;

	public static void setLogLevel(Level l) {
		UniqFieldNameValueIterator.log.setLevel(l);
	}

	public UniqFieldNameValueIterator(Text fName, Text fValLower, Text fValUpper) {
		this.fieldName = fName;
		this.fieldValueLowerBound = fValLower;
		this.fieldValueUpperBound = fValUpper;
		keyParser = createDefaultKeyParser();
	}

	public UniqFieldNameValueIterator(UniqFieldNameValueIterator other, IteratorEnvironment env) {
		source = other.getSource().deepCopy(env);
		keyParser = createDefaultKeyParser();
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		source = super.getSource();
	}

	@Override
	protected void setSource(SortedKeyValueIterator<Key, Value> source) {
		this.source = source;
	}

	@Override
	protected SortedKeyValueIterator<Key, Value> getSource() {
		return source;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new UniqFieldNameValueIterator(this, env);
	}

	@Override
	public Key getTopKey() {
		return this.topKey;
	}

	@Override
	public Value getTopValue() {
		return this.topValue;
	}

	@Override
	public boolean hasTop() {
		return (topKey) != null;
	}

	@Override
	public void next() throws IOException {
		if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
			UniqFieldNameValueIterator.log.debug("next()");
		}
		if (!(source.hasTop())) {
			topKey = null;
			topValue = null;
			return;
		}
		Key currentKey = topKey;
		keyParser.parse(topKey);
		String fValue = keyParser.getFieldValue();
		Text currentRow = currentKey.getRow();
		Text currentFam = currentKey.getColumnFamily();
		if (((overallRange.getEndKey()) != null) && ((overallRange.getEndKey().getRow().compareTo(currentRow)) < 0)) {
			if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
				UniqFieldNameValueIterator.log.debug(((("next, overall endRow: " + (overallRange.getEndKey().getRow())) + "  currentRow: ") + currentRow));
			}
			topKey = null;
			topValue = null;
			return;
		}
		if ((fValue.compareTo(this.fieldValueUpperBound.toString())) > 0) {
			topKey = null;
			topValue = null;
			return;
		}
		Key followingKey = new Key(currentKey.getRow(), this.fieldName, new Text((fValue + (UniqFieldNameValueIterator.ONE_BYTE))));
		if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
			UniqFieldNameValueIterator.log.debug(("next, followingKey to seek on: " + followingKey));
		}
		Range r = new Range(followingKey, followingKey);
		source.seek(r, UniqFieldNameValueIterator.EMPTY_COL_FAMS, false);
		while (true) {
			if (!(source.hasTop())) {
				topKey = null;
				topValue = null;
				return;
			}
			Key k = source.getTopKey();
			if (!(overallRange.contains(k))) {
				topKey = null;
				topValue = null;
				return;
			}
			if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
				UniqFieldNameValueIterator.log.debug(((("next(), key: " + k) + " subrange: ") + (this.currentSubRange)));
			}
			keyParser.parse(k);
			Text currentVal = new Text(keyParser.getFieldValue());
			if (((k.getRow().equals(currentRow)) && (k.getColumnFamily().equals(currentFam))) && ((currentVal.compareTo(fieldValueUpperBound)) <= 0)) {
				topKey = k;
				topValue = source.getTopValue();
				return;
			}else {
				if ((this.overallRange.contains(k)) && (this.multiRow)) {
					if (k.getRow().equals(currentRow)) {
						currentRow = getNextRow();
						if (currentRow == null) {
							topKey = null;
							topValue = null;
							return;
						}
					}else {
						currentRow = source.getTopKey().getRow();
					}
					Key sKey = new Key(currentRow, fieldName, fieldValueLowerBound);
					Key eKey = new Key(currentRow, fieldName, fieldValueUpperBound);
					currentSubRange = new Range(sKey, eKey);
					source.seek(currentSubRange, UniqFieldNameValueIterator.EMPTY_COL_FAMS, seekInclusive);
				}else {
					topKey = null;
					topValue = null;
					return;
				}
			}
		} 
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
			UniqFieldNameValueIterator.log.debug(("seek, range: " + range));
		}
		this.overallRange = range;
		this.seekInclusive = inclusive;
		source.seek(range, UniqFieldNameValueIterator.EMPTY_COL_FAMS, inclusive);
		topKey = null;
		topValue = null;
		Key sKey;
		Key eKey;
		if (range.isInfiniteStartKey()) {
			sKey = source.getTopKey();
			if (sKey == null) {
				return;
			}
		}else {
			sKey = range.getStartKey();
		}
		if (range.isInfiniteStopKey()) {
			eKey = null;
			this.multiRow = true;
		}else {
			eKey = range.getEndKey();
			if (sKey.getRow().equals(eKey.getRow())) {
				this.multiRow = false;
			}else {
				this.multiRow = true;
			}
		}
		if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
			UniqFieldNameValueIterator.log.debug(((("seek, multiRow:" + (multiRow)) + " range:") + range));
		}
		Text sRow = sKey.getRow();
		Key ssKey = new Key(sRow, this.fieldName, this.fieldValueLowerBound);
		Key eeKey = new Key(sRow, this.fieldName, this.fieldValueUpperBound);
		this.currentSubRange = new Range(ssKey, eeKey);
		if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
			UniqFieldNameValueIterator.log.debug(("seek, currentSubRange: " + (currentSubRange)));
		}
		source.seek(this.currentSubRange, columnFamilies, inclusive);
		while ((topKey) == null) {
			if (source.hasTop()) {
				Key k = source.getTopKey();
				if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
					UniqFieldNameValueIterator.log.debug(("seek, source.topKey: " + k));
				}
				if (currentSubRange.contains(k)) {
					topKey = k;
					topValue = source.getTopValue();
					if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
						UniqFieldNameValueIterator.log.debug("seek, source has top in valid range");
					}
				}else {
					if ((multiRow) && (overallRange.contains(k))) {
						Key fKey = sKey.followingKey(PartialKey.ROW);
						Range fRange = new Range(fKey, eKey);
						source.seek(fRange, columnFamilies, inclusive);
						if (source.hasTop()) {
							Text row = source.getTopKey().getRow();
							Key nKey = new Key(row, this.fieldName, this.fieldValueLowerBound);
							this.currentSubRange = new Range(nKey, eKey);
							sKey = this.currentSubRange.getStartKey();
							Range nextRange = new Range(sKey, eKey);
							source.seek(nextRange, columnFamilies, inclusive);
						}else {
							topKey = null;
							topValue = null;
							return;
						}
					}else {
						topKey = null;
						topValue = null;
						return;
					}
				}
			}else {
				topKey = null;
				topValue = null;
				return;
			}
		} 
	}

	private FieldIndexKeyParser createDefaultKeyParser() {
		FieldIndexKeyParser parser = new FieldIndexKeyParser();
		return parser;
	}

	private Text getNextRow() throws IOException {
		if (UniqFieldNameValueIterator.log.isDebugEnabled()) {
			UniqFieldNameValueIterator.log.debug("getNextRow()");
		}
		Key fakeKey = new Key(source.getTopKey().followingKey(PartialKey.ROW));
		Range fakeRange = new Range(fakeKey, fakeKey);
		source.seek(fakeRange, UniqFieldNameValueIterator.EMPTY_COL_FAMS, false);
		if (source.hasTop()) {
			return source.getTopKey().getRow();
		}else {
			return null;
		}
	}
}

