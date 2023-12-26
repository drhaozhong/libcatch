package org.apache.accumulo.examples.wikisearch.iterator;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class AndIterator implements SortedKeyValueIterator<Key, Value> {
	protected static final Logger log = Logger.getLogger(AndIterator.class);

	private AndIterator.TermSource[] sources;

	private int sourcesCount = 0;

	protected Text nullText = new Text();

	protected final byte[] emptyByteArray = new byte[0];

	private Key topKey = null;

	protected Value value = new Value(emptyByteArray);

	private Range overallRange;

	private Text currentRow = null;

	private Text currentTerm = new Text(emptyByteArray);

	private Text currentDocID = new Text(emptyByteArray);

	private Text parentEndRow;

	private static boolean SEEK_INCLUSIVE = true;

	protected static class TermSource {
		public SortedKeyValueIterator<Key, Value> iter;

		public Text dataLocation;

		public Text term;

		public boolean notFlag;

		private Collection<ByteSequence> seekColumnFamilies;

		private TermSource(AndIterator.TermSource other) {
			this(other.iter, other.dataLocation, other.term, other.notFlag);
		}

		public TermSource(SortedKeyValueIterator<Key, Value> iter, Text dataLocation, Text term) {
			this(iter, dataLocation, term, false);
		}

		public TermSource(SortedKeyValueIterator<Key, Value> iter, Text dataLocation, Text term, boolean notFlag) {
			this.iter = iter;
			this.dataLocation = dataLocation;
			ByteSequence bs = new ArrayByteSequence(dataLocation.getBytes(), 0, dataLocation.getLength());
			this.seekColumnFamilies = Collections.singletonList(bs);
			this.term = term;
			this.notFlag = notFlag;
		}

		public String getTermString() {
			return (this.term) == null ? new String("Iterator") : this.term.toString();
		}
	}

	protected Text getPartition(Key key) {
		return key.getRow();
	}

	protected Text getDataLocation(Key key) {
		return key.getColumnFamily();
	}

	protected Text getTerm(Key key) {
		int idx = 0;
		String sKey = key.getColumnQualifier().toString();
		idx = sKey.indexOf("\u0000");
		return new Text(sKey.substring(0, idx));
	}

	protected Text getDocID(Key key) {
		int idx = 0;
		String sKey = key.getColumnQualifier().toString();
		idx = sKey.indexOf("\u0000");
		return new Text(sKey.substring((idx + 1)));
	}

	protected String getUID(Key key) {
		int idx = 0;
		String sKey = key.getColumnQualifier().toString();
		idx = sKey.indexOf("\u0000");
		return sKey.substring((idx + 1));
	}

	protected Key buildKey(Text row, Text dataLocation) {
		return new Key(row, (dataLocation == null ? nullText : dataLocation));
	}

	protected Key buildKey(Text row, Text dataLocation, Text term) {
		return new Key(row, (dataLocation == null ? nullText : dataLocation), (term == null ? nullText : term));
	}

	protected Key buildFollowingPartitionKey(Key key) {
		return key.followingKey(PartialKey.ROW);
	}

	public AndIterator() {
	}

	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new AndIterator(this, env);
	}

	public AndIterator(AndIterator other, IteratorEnvironment env) {
		if ((other.sources) != null) {
			sourcesCount = other.sourcesCount;
			sources = new AndIterator.TermSource[sourcesCount];
			for (int i = 0; i < (sourcesCount); i++) {
				sources[i] = new AndIterator.TermSource(other.sources[i].iter.deepCopy(env), other.sources[i].dataLocation, other.sources[i].term);
			}
		}
	}

	public Key getTopKey() {
		return topKey;
	}

	public Value getTopValue() {
		return value;
	}

	public boolean hasTop() {
		return (currentRow) != null;
	}

	private boolean seekOneSource(AndIterator.TermSource ts) throws IOException {
		boolean advancedCursor = false;
		while (true) {
			if ((ts.iter.hasTop()) == false) {
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("The current iterator no longer has a top");
				}
				if (ts.notFlag) {
					break;
				}
				currentRow = null;
				return true;
			}
			int endCompare = -1;
			if (AndIterator.log.isDebugEnabled()) {
				AndIterator.log.debug(("Current topKey = " + (ts.iter.getTopKey())));
			}
			if ((overallRange.getEndKey()) != null) {
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("II.seekOneSource overallRange.getEndKey() != null");
				}
				endCompare = overallRange.getEndKey().getRow().compareTo(ts.iter.getTopKey().getRow());
				if (((!(overallRange.isEndKeyInclusive())) && (endCompare <= 0)) || (endCompare < 0)) {
					if (AndIterator.log.isDebugEnabled()) {
						AndIterator.log.debug("II.seekOneSource at the end of the tablet server");
					}
					currentRow = null;
					return true;
				}
			}else {
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("II.seekOneSource overallRange.getEndKey() == null");
				}
			}
			int partitionCompare = currentRow.compareTo(getPartition(ts.iter.getTopKey()));
			if (AndIterator.log.isDebugEnabled()) {
				AndIterator.log.debug(("Current partition: " + (currentRow)));
			}
			if (partitionCompare > 0) {
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("Need to seek to the current row");
					AndIterator.log.debug(("ts.dataLocation = " + (ts.dataLocation.getBytes())));
					AndIterator.log.debug(("Term = " + (new Text((((ts.term) + "\u0000") + (currentDocID))).getBytes())));
				}
				Key seekKey = buildKey(currentRow, ts.dataLocation, nullText);
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug(("Seeking to: " + seekKey));
				}
				ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamilies, AndIterator.SEEK_INCLUSIVE);
				continue;
			}
			if (partitionCompare < 0) {
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("Went too far beyond the currentRow");
				}
				if (ts.notFlag) {
					break;
				}
				currentRow.set(getPartition(ts.iter.getTopKey()));
				currentDocID.set(emptyByteArray);
				advancedCursor = true;
				continue;
			}
			if ((ts.dataLocation) != null) {
				int dataLocationCompare = ts.dataLocation.compareTo(getDataLocation(ts.iter.getTopKey()));
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("Comparing dataLocations");
					AndIterator.log.debug(("dataLocation = " + (ts.dataLocation)));
					AndIterator.log.debug(("newDataLocation = " + (getDataLocation(ts.iter.getTopKey()))));
				}
				if (dataLocationCompare > 0) {
					if (AndIterator.log.isDebugEnabled()) {
						AndIterator.log.debug("Need to seek to the right dataLocation");
					}
					Key seekKey = buildKey(currentRow, ts.dataLocation, nullText);
					if (AndIterator.log.isDebugEnabled()) {
						AndIterator.log.debug(("Seeking to: " + seekKey));
					}
					ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamilies, AndIterator.SEEK_INCLUSIVE);
					if (!(ts.iter.hasTop())) {
						currentRow = null;
						return true;
					}
					continue;
				}
				if (dataLocationCompare < 0) {
					if (AndIterator.log.isDebugEnabled()) {
						AndIterator.log.debug("Went too far beyond the dataLocation");
					}
					if (endCompare == 0) {
						currentRow = null;
						return true;
					}
					if (ts.notFlag) {
						break;
					}
					Key seekKey = buildFollowingPartitionKey(ts.iter.getTopKey());
					if (AndIterator.log.isDebugEnabled()) {
						AndIterator.log.debug(("Seeking to: " + seekKey));
					}
					ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamilies, AndIterator.SEEK_INCLUSIVE);
					if (!(ts.iter.hasTop())) {
						currentRow = null;
						return true;
					}
					continue;
				}
			}
			int termCompare = ts.term.compareTo(getTerm(ts.iter.getTopKey()));
			if (AndIterator.log.isDebugEnabled()) {
				AndIterator.log.debug(("term = " + (ts.term)));
				AndIterator.log.debug(("newTerm = " + (getTerm(ts.iter.getTopKey()))));
			}
			if (termCompare > 0) {
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("Need to seek to the right term");
				}
				Key seekKey = buildKey(currentRow, ts.dataLocation, new Text(((ts.term) + "\u0000")));
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug(("Seeking to: " + seekKey));
				}
				ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamilies, AndIterator.SEEK_INCLUSIVE);
				if (!(ts.iter.hasTop())) {
					currentRow = null;
					return true;
				}
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug(("topKey after seeking to correct term: " + (ts.iter.getTopKey())));
				}
				continue;
			}
			if (termCompare < 0) {
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("TERM: Need to jump to the next row");
				}
				if (endCompare == 0) {
					currentRow = null;
					return true;
				}
				if (ts.notFlag) {
					break;
				}
				Key seekKey = buildFollowingPartitionKey(ts.iter.getTopKey());
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug(("Using this key to find the next key: " + (ts.iter.getTopKey())));
					AndIterator.log.debug(("Seeking to: " + seekKey));
				}
				ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamilies, AndIterator.SEEK_INCLUSIVE);
				if (!(ts.iter.hasTop())) {
					currentRow = null;
					return true;
				}
				currentTerm = getTerm(ts.iter.getTopKey());
				continue;
			}
			Text docid = getDocID(ts.iter.getTopKey());
			int docidCompare = currentDocID.compareTo(docid);
			if (AndIterator.log.isDebugEnabled()) {
				AndIterator.log.debug("Comparing DocIDs");
				AndIterator.log.debug(("currentDocID = " + (currentDocID)));
				AndIterator.log.debug(("docid = " + docid));
			}
			if (docidCompare > 0) {
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("Need to seek to the correct docid");
				}
				Key seekKey = buildKey(currentRow, ts.dataLocation, new Text((((ts.term) + "\u0000") + (currentDocID))));
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug(("Seeking to: " + seekKey));
				}
				ts.iter.seek(new Range(seekKey, true, null, false), ts.seekColumnFamilies, AndIterator.SEEK_INCLUSIVE);
				continue;
			}
			if (docidCompare < 0) {
				if (ts.notFlag) {
					break;
				}
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("We went too far, update the currentDocID to be the location of where were seek'ed to");
				}
				currentDocID.set(docid);
				advancedCursor = true;
				break;
			}
			currentTerm = getTerm(ts.iter.getTopKey());
			if (AndIterator.log.isDebugEnabled()) {
				AndIterator.log.debug(("currentTerm = " + (currentTerm)));
			}
			if (ts.notFlag) {
				sources[0].iter.next();
				advancedCursor = true;
			}
			break;
		} 
		return advancedCursor;
	}

	public void next() throws IOException {
		if (AndIterator.log.isDebugEnabled()) {
			AndIterator.log.debug("In ModifiedIntersectingIterator.next()");
		}
		if ((currentRow) == null) {
			return;
		}
		sources[0].iter.next();
		advanceToIntersection();
		if (hasTop()) {
			if (((overallRange) != null) && (!(overallRange.contains(topKey)))) {
				topKey = null;
			}
		}
	}

	protected void advanceToIntersection() throws IOException {
		if (AndIterator.log.isDebugEnabled()) {
			AndIterator.log.debug("In AndIterator.advanceToIntersection()");
		}
		boolean cursorChanged = true;
		while (cursorChanged) {
			cursorChanged = false;
			for (AndIterator.TermSource ts : sources) {
				if ((currentRow) == null) {
					topKey = null;
					return;
				}
				if (seekOneSource(ts)) {
					cursorChanged = true;
					break;
				}
			}
		} 
		topKey = buildKey(currentRow, currentTerm, currentDocID);
		if (AndIterator.log.isDebugEnabled()) {
			AndIterator.log.debug(("ModifiedIntersectingIterator: Got a match: " + (topKey)));
		}
	}

	public static String stringTopKey(SortedKeyValueIterator<Key, Value> iter) {
		if (iter.hasTop()) {
			return iter.getTopKey().toString();
		}
		return "";
	}

	public static final String columnFamiliesOptionName = "columnFamilies";

	public static final String termValuesOptionName = "termValues";

	public static final String notFlagsOptionName = "notFlags";

	public static String encodeColumns(Text[] columns) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < (columns.length); i++) {
			sb.append(new String(Base64.encodeBase64(TextUtil.getBytes(columns[i]))));
			sb.append('\n');
		}
		return sb.toString();
	}

	public static String encodeTermValues(Text[] terms) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < (terms.length); i++) {
			sb.append(new String(Base64.encodeBase64(TextUtil.getBytes(terms[i]))));
			sb.append('\n');
		}
		return sb.toString();
	}

	public static String encodeBooleans(boolean[] flags) {
		byte[] bytes = new byte[flags.length];
		for (int i = 0; i < (flags.length); i++) {
			if (flags[i]) {
				bytes[i] = 1;
			}else {
				bytes[i] = 0;
			}
		}
		return new String(Base64.encodeBase64(bytes));
	}

	public static Text[] decodeColumns(String columns) {
		String[] columnStrings = columns.split("\n");
		Text[] columnTexts = new Text[columnStrings.length];
		for (int i = 0; i < (columnStrings.length); i++) {
			columnTexts[i] = new Text(Base64.decodeBase64(columnStrings[i].getBytes()));
		}
		return columnTexts;
	}

	public static Text[] decodeTermValues(String terms) {
		String[] termStrings = terms.split("\n");
		Text[] termTexts = new Text[termStrings.length];
		for (int i = 0; i < (termStrings.length); i++) {
			termTexts[i] = new Text(Base64.decodeBase64(termStrings[i].getBytes()));
		}
		return termTexts;
	}

	public static boolean[] decodeBooleans(String flags) {
		if (flags == null) {
			return null;
		}
		byte[] bytes = Base64.decodeBase64(flags.getBytes());
		boolean[] bFlags = new boolean[bytes.length];
		for (int i = 0; i < (bytes.length); i++) {
			if ((bytes[i]) == 1) {
				bFlags[i] = true;
			}else {
				bFlags[i] = false;
			}
		}
		return bFlags;
	}

	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		if (AndIterator.log.isDebugEnabled()) {
			AndIterator.log.debug("In AndIterator.init()");
		}
		Text[] dataLocations = AndIterator.decodeColumns(options.get(AndIterator.columnFamiliesOptionName));
		Text[] terms = AndIterator.decodeTermValues(options.get(AndIterator.termValuesOptionName));
		boolean[] notFlags = AndIterator.decodeBooleans(options.get(AndIterator.notFlagsOptionName));
		if ((terms.length) < 2) {
			throw new IllegalArgumentException("AndIterator requires two or more columns families");
		}
		if (notFlags == null) {
			notFlags = new boolean[terms.length];
			for (int i = 0; i < (terms.length); i++) {
				notFlags[i] = false;
			}
		}
		if (notFlags[0]) {
			for (int i = 1; i < (notFlags.length); i++) {
				if ((notFlags[i]) == false) {
					Text swap = new Text(terms[0]);
					terms[0].set(terms[i]);
					terms[i].set(swap);
					swap.set(dataLocations[0]);
					dataLocations[0].set(dataLocations[i]);
					dataLocations[i].set(swap);
					notFlags[0] = false;
					notFlags[i] = true;
					break;
				}
			}
			if (notFlags[0]) {
				throw new IllegalArgumentException("AndIterator requires at least one column family without not");
			}
		}
		sources = new AndIterator.TermSource[dataLocations.length];
		for (int i = 0; i < (dataLocations.length); i++) {
			sources[i] = new AndIterator.TermSource(source.deepCopy(env), dataLocations[i], terms[i], notFlags[i]);
		}
		sourcesCount = dataLocations.length;
	}

	public void seek(Range range, Collection<ByteSequence> seekColumnFamilies, boolean inclusive) throws IOException {
		if (AndIterator.log.isDebugEnabled()) {
			AndIterator.log.debug("In AndIterator.seek()");
			AndIterator.log.debug(("AndIterator.seek Given range => " + range));
		}
		currentRow = new Text();
		currentDocID.set(emptyByteArray);
		doSeek(range);
	}

	private void doSeek(Range range) throws IOException {
		overallRange = new Range(range);
		if (((range.getEndKey()) != null) && ((range.getEndKey().getRow()) != null)) {
			this.parentEndRow = range.getEndKey().getRow();
		}
		for (int i = 0; i < (sourcesCount); i++) {
			Key sourceKey;
			Text dataLocation = ((sources[i].dataLocation) == null) ? nullText : sources[i].dataLocation;
			if ((range.getStartKey()) != null) {
				if ((range.getStartKey().getColumnFamily()) != null) {
					sourceKey = buildKey(getPartition(range.getStartKey()), dataLocation, ((sources[i].term) == null ? nullText : new Text((((sources[i].term) + "\u0000") + (range.getStartKey().getColumnFamily())))));
				}else {
					sourceKey = buildKey(getPartition(range.getStartKey()), dataLocation, ((sources[i].term) == null ? nullText : sources[i].term));
				}
				if (!(range.isStartKeyInclusive()))
					sourceKey = sourceKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL);

				sources[i].iter.seek(new Range(sourceKey, true, null, false), sources[i].seekColumnFamilies, AndIterator.SEEK_INCLUSIVE);
			}else {
				sources[i].iter.seek(range, sources[i].seekColumnFamilies, AndIterator.SEEK_INCLUSIVE);
			}
		}
		advanceToIntersection();
		if (hasTop()) {
			if (((overallRange) != null) && (!(overallRange.contains(topKey)))) {
				topKey = null;
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug(("doSeek, topKey is outside of overall range: " + (overallRange)));
				}
			}
		}
	}

	public void addSource(SortedKeyValueIterator<Key, Value> source, IteratorEnvironment env, Text term, boolean notFlag) {
		addSource(source, env, null, term, notFlag);
	}

	public void addSource(SortedKeyValueIterator<Key, Value> source, IteratorEnvironment env, Text dataLocation, Text term, boolean notFlag) {
		if ((sources) == null) {
			sources = new AndIterator.TermSource[1];
		}else {
			AndIterator.TermSource[] localSources = new AndIterator.TermSource[(sources.length) + 1];
			int currSource = 0;
			for (AndIterator.TermSource myTerm : sources) {
				localSources[currSource] = new AndIterator.TermSource(myTerm);
				currSource++;
			}
			sources = localSources;
		}
		sources[sourcesCount] = new AndIterator.TermSource(source.deepCopy(env), dataLocation, term, notFlag);
		(sourcesCount)++;
	}

	public boolean jump(Key jumpKey) throws IOException {
		if (AndIterator.log.isDebugEnabled()) {
			AndIterator.log.debug(("jump: " + jumpKey));
		}
		if (((parentEndRow) != null) && ((parentEndRow.compareTo(jumpKey.getRow())) < 0)) {
			if (AndIterator.log.isDebugEnabled()) {
				AndIterator.log.debug(((("jumpRow: " + (jumpKey.getRow())) + " is greater than my parentEndRow: ") + (parentEndRow)));
			}
			return false;
		}
		if (!(hasTop())) {
			if (AndIterator.log.isDebugEnabled()) {
				AndIterator.log.debug("jump called, but topKey is null, must need to move to next row");
			}
			return false;
		}else {
			int comp = this.topKey.getRow().compareTo(jumpKey.getRow());
			if (comp > 0) {
				if (AndIterator.log.isDebugEnabled()) {
					AndIterator.log.debug("jump, our row is ahead of jumpKey.");
					AndIterator.log.debug(((((("jumpRow: " + (jumpKey.getRow())) + " myRow: ") + (topKey.getRow())) + " parentEndRow") + (parentEndRow)));
				}
				return hasTop();
			}else
				if (comp < 0) {
					if (AndIterator.log.isDebugEnabled()) {
						AndIterator.log.debug("II jump, row jump");
					}
					Key endKey = null;
					if ((parentEndRow) != null) {
						endKey = new Key(parentEndRow);
					}
					Key sKey = new Key(jumpKey.getRow());
					Range fake = new Range(sKey, true, endKey, false);
					this.seek(fake, null, false);
					return hasTop();
				}else {
					String myUid = this.topKey.getColumnQualifier().toString();
					String jumpUid = getUID(jumpKey);
					if (AndIterator.log.isDebugEnabled()) {
						if (myUid == null) {
							AndIterator.log.debug("myUid is null");
						}else {
							AndIterator.log.debug(("myUid: " + myUid));
						}
						if (jumpUid == null) {
							AndIterator.log.debug("jumpUid is null");
						}else {
							AndIterator.log.debug(("jumpUid: " + jumpUid));
						}
					}
					int ucomp = myUid.compareTo(jumpUid);
					if (ucomp < 0) {
						if (AndIterator.log.isDebugEnabled()) {
							AndIterator.log.debug("jump, uid jump");
						}
						Text row = jumpKey.getRow();
						Range range = new Range(row);
						this.currentRow = row;
						this.currentDocID = new Text(this.getUID(jumpKey));
						doSeek(range);
						if (((hasTop()) && ((parentEndRow) != null)) && ((topKey.getRow().compareTo(parentEndRow)) > 0)) {
							topKey = null;
						}
						if ((AndIterator.log.isDebugEnabled()) && (hasTop())) {
							AndIterator.log.debug(("jump, topKey is now: " + (topKey)));
						}
						return hasTop();
					}
					if (((hasTop()) && ((parentEndRow) != null)) && ((topKey.getRow().compareTo(parentEndRow)) > 0)) {
						topKey = null;
					}
					return hasTop();
				}

		}
	}
}

