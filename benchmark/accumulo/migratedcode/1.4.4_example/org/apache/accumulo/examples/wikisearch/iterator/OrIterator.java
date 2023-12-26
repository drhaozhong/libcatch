package org.apache.accumulo.examples.wikisearch.iterator;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.examples.wikisearch.iterator.OrIterator.TermSource;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class OrIterator implements SortedKeyValueIterator<Key, Value> {
	private OrIterator.TermSource currentTerm;

	private ArrayList<OrIterator.TermSource> sources;

	private PriorityQueue<OrIterator.TermSource> sorted = new PriorityQueue<OrIterator.TermSource>(5);

	private static final Text nullText = new Text();

	private Key topKey = null;

	private Range overallRange;

	private Collection<ByteSequence> columnFamilies;

	private boolean inclusive;

	protected static final Logger log = Logger.getLogger(OrIterator.class);

	private Text parentEndRow;

	protected static class TermSource implements Comparable<OrIterator.TermSource> {
		public SortedKeyValueIterator<Key, Value> iter;

		public Text dataLocation;

		public Text term;

		public Text docid;

		public Text fieldTerm;

		public Key topKey;

		public boolean atEnd;

		public TermSource(OrIterator.TermSource other) {
			this.iter = other.iter;
			this.term = other.term;
			this.dataLocation = other.dataLocation;
			this.atEnd = other.atEnd;
		}

		public TermSource(SortedKeyValueIterator<Key, Value> iter, Text term) {
			this.iter = iter;
			this.term = term;
			this.atEnd = false;
		}

		public TermSource(SortedKeyValueIterator<Key, Value> iter, Text dataLocation, Text term) {
			this.iter = iter;
			this.dataLocation = dataLocation;
			this.term = term;
			this.atEnd = false;
		}

		public void setNew() {
			if ((!(this.atEnd)) && (this.iter.hasTop())) {
				this.topKey = this.iter.getTopKey();
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug((("OI.TermSource.setNew TS.iter.topKey >>" + (topKey)) + "<<"));
				}
				if ((this.term) == null) {
					this.docid = this.topKey.getColumnQualifier();
				}else {
					String cqString = this.topKey.getColumnQualifier().toString();
					int idx = cqString.indexOf("\u0000");
					this.fieldTerm = new Text(cqString.substring(0, idx));
					this.docid = new Text(cqString.substring((idx + 1)));
				}
			}else {
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug("OI.TermSource.setNew Setting to null...");
				}
				this.topKey = null;
				this.fieldTerm = null;
				this.docid = null;
			}
		}

		public int compareTo(OrIterator.TermSource o) {
			Key k1 = topKey;
			Key k2 = o.topKey;
			String uid1 = OrIterator.getUID(k1);
			String uid2 = OrIterator.getUID(k2);
			if ((uid1 != null) && (uid2 != null)) {
				return uid1.compareTo(uid2);
			}else
				if ((uid1 == null) && (uid2 == null)) {
					return 0;
				}else
					if (uid1 == null) {
						return 1;
					}else {
						return -1;
					}


		}

		@Override
		public String toString() {
			return (("TermSource: " + (this.dataLocation)) + " ") + (this.term);
		}

		public boolean hasTop() {
			return (this.topKey) != null;
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

	protected static String getUID(Key key) {
		try {
			int idx = 0;
			String sKey = key.getColumnQualifier().toString();
			idx = sKey.indexOf("\u0000");
			return sKey.substring((idx + 1));
		} catch (Exception e) {
			return null;
		}
	}

	public OrIterator() {
		this.sources = new ArrayList<OrIterator.TermSource>();
	}

	private OrIterator(OrIterator other, IteratorEnvironment env) {
		this.sources = new ArrayList<OrIterator.TermSource>();
		for (OrIterator.TermSource TS : other.sources) {
			this.sources.add(new OrIterator.TermSource(TS.iter.deepCopy(env), TS.dataLocation, TS.term));
		}
	}

	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new OrIterator(this, env);
	}

	public void addTerm(SortedKeyValueIterator<Key, Value> source, Text term, IteratorEnvironment env) {
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug("OI.addTerm Added source w/o family");
			OrIterator.log.debug((("OI.addTerm term >>" + term) + "<<"));
		}
		if (term == null) {
			this.sources.add(new OrIterator.TermSource(source, term));
		}else {
			this.sources.add(new OrIterator.TermSource(source.deepCopy(env), term));
		}
	}

	public void addTerm(SortedKeyValueIterator<Key, Value> source, Text dataLocation, Text term, IteratorEnvironment env) {
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug("OI.addTerm Added source ");
			OrIterator.log.debug((((("OI.addTerm family >>" + dataLocation) + "<<      term >>") + term) + "<<"));
		}
		if (term == null) {
			this.sources.add(new OrIterator.TermSource(source, dataLocation, term));
		}else {
			this.sources.add(new OrIterator.TermSource(source.deepCopy(env), dataLocation, term));
		}
	}

	protected Key buildTopKey(OrIterator.TermSource TS) {
		if ((TS == null) || ((TS.topKey) == null)) {
			return null;
		}
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug((("OI.buildTopKey New topKey >>" + (new Key(TS.topKey.getRow(), TS.dataLocation, TS.docid))) + "<<"));
		}
		return new Key(TS.topKey.getRow(), TS.topKey.getColumnFamily(), TS.topKey.getColumnQualifier());
	}

	public final void next() throws IOException {
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(((("OI.next Enter: sorted.size = " + (sorted.size())) + " currentTerm = ") + ((currentTerm) == null ? "null" : "not null")));
		}
		if ((currentTerm) == null) {
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug("OI.next currentTerm is NULL... returning");
			}
			topKey = null;
			return;
		}
		currentTerm.iter.next();
		advanceToMatch(currentTerm);
		currentTerm.setNew();
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(((("OI.next Checks (correct = 0,0,0): " + ((currentTerm.topKey) != null ? "0," : "1,")) + ((currentTerm.dataLocation) != null ? "0," : "1,")) + (((currentTerm.term) != null) && ((currentTerm.fieldTerm) != null) ? currentTerm.term.compareTo(currentTerm.fieldTerm) : "0")));
		}
		if (((currentTerm.topKey) == null) || (((currentTerm.dataLocation) != null) && ((currentTerm.term.compareTo(currentTerm.fieldTerm)) != 0))) {
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug(("OI.next removing entry:" + (currentTerm.term)));
			}
			currentTerm = null;
		}
		if ((sorted.size()) > 0) {
			if ((currentTerm) != null) {
				sorted.add(currentTerm);
			}
			currentTerm = sorted.poll();
		}
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(("OI.next CurrentTerm is " + ((currentTerm) == null ? "null" : currentTerm)));
		}
		topKey = buildTopKey(currentTerm);
		if (hasTop()) {
			if (((overallRange) != null) && (!(overallRange.contains(topKey)))) {
				topKey = null;
			}
		}
	}

	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		overallRange = new Range(range);
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(("seek, overallRange: " + (overallRange)));
		}
		if (((range.getEndKey()) != null) && ((range.getEndKey().getRow()) != null)) {
			this.parentEndRow = range.getEndKey().getRow();
		}
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(("OI.seek Entry - sources.size = " + (sources.size())));
			OrIterator.log.debug(("OI.seek Entry - currentTerm = " + ((currentTerm) == null ? "false" : currentTerm.iter.getTopKey())));
			OrIterator.log.debug(("OI.seek Entry - Key from Range = " + (range == null ? "false" : range.getStartKey())));
		}
		if (sources.isEmpty()) {
			currentTerm = null;
			topKey = null;
			return;
		}
		this.columnFamilies = columnFamilies;
		this.inclusive = inclusive;
		Range newRange = range;
		Key sourceKey = null;
		Key startKey = null;
		if (range != null) {
			startKey = range.getStartKey();
		}
		sorted.clear();
		OrIterator.TermSource TS = null;
		Iterator<OrIterator.TermSource> iter = sources.iterator();
		int counter = 1;
		while (iter.hasNext()) {
			TS = iter.next();
			TS.atEnd = false;
			if ((sources.size()) == 1) {
				currentTerm = TS;
			}
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug((("OI.seek on TS >>" + TS) + "<<"));
				OrIterator.log.debug((("OI.seek seeking source >>" + counter) + "<< "));
			}
			counter++;
			newRange = range;
			sourceKey = null;
			if (startKey != null) {
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug((("OI.seek startKey >>" + startKey) + "<<"));
				}
				if ((startKey.getColumnQualifier()) != null) {
					sourceKey = new Key(startKey.getRow(), ((TS.dataLocation) == null ? OrIterator.nullText : TS.dataLocation), new Text((((TS.term) == null ? "" : (TS.term) + "\u0000") + (range.getStartKey().getColumnQualifier()))));
				}else {
					sourceKey = new Key(startKey.getRow(), ((TS.dataLocation) == null ? OrIterator.nullText : TS.dataLocation), ((TS.term) == null ? OrIterator.nullText : TS.term));
				}
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug(("OI.seek Seeking to the key => " + sourceKey));
				}
				newRange = new Range(sourceKey, true, sourceKey.followingKey(PartialKey.ROW), false);
			}else {
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug(("OI.seek Using the range Seek() argument to seek => " + newRange));
				}
			}
			TS.iter.seek(newRange, columnFamilies, inclusive);
			TS.setNew();
			advanceToMatch(TS);
			TS.setNew();
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug((("OI.seek sourceKey >>" + sourceKey) + "<< "));
				OrIterator.log.debug((("OI.seek topKey >>" + ((TS.topKey) == null ? "false" : TS.topKey)) + "<< "));
				OrIterator.log.debug(("OI.seek TS.fieldTerm == " + (TS.fieldTerm)));
				OrIterator.log.debug(((("OI.seek Checks (correct = 0,0,0 / 0,1,1): " + ((TS.topKey) != null ? "0," : "1,")) + ((TS.dataLocation) != null ? "0," : "1,")) + ((((TS.term) != null) && ((TS.fieldTerm) != null)) && ((TS.term.compareTo(TS.fieldTerm)) != 0) ? "0" : "1")));
			}
			if (((TS.topKey) == null) || (((TS.dataLocation) != null) && ((TS.term.compareTo(TS.fieldTerm)) != 0))) {
			}else
				if (((sources.size()) > 0) || (iter.hasNext())) {
					sorted.add(TS);
				}else {
					if (OrIterator.log.isDebugEnabled()) {
						OrIterator.log.debug((("OI.seek new topKey >>" + ((topKey) == null ? "false" : topKey)) + "<< "));
					}
					if (hasTop()) {
						if (((overallRange) != null) && (!(overallRange.contains(topKey)))) {
							if (OrIterator.log.isDebugEnabled()) {
								OrIterator.log.debug(((("seek, topKey: " + (topKey)) + " is not in the overallRange: ") + (overallRange)));
							}
							topKey = null;
						}
					}
					return;
				}

		} 
		currentTerm = sorted.poll();
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(("OI.seek currentTerm = " + (currentTerm)));
		}
		topKey = buildTopKey(currentTerm);
		if ((topKey) == null) {
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug("OI.seek() topKey is null");
			}
		}
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug((("OI.seek new topKey >>" + ((topKey) == null ? "false" : topKey)) + "<< "));
		}
		if (hasTop()) {
			if (((overallRange) != null) && (!(overallRange.contains(topKey)))) {
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug(((("seek, topKey: " + (topKey)) + " is not in the overallRange: ") + (overallRange)));
				}
				topKey = null;
			}
		}
	}

	public final Key getTopKey() {
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(("OI.getTopKey key >>" + (topKey)));
		}
		return topKey;
	}

	public final Value getTopValue() {
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(("OI.getTopValue key >>" + (currentTerm.iter.getTopValue())));
		}
		return currentTerm.iter.getTopValue();
	}

	public final boolean hasTop() {
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(("OI.hasTop  =  " + ((topKey) == null ? "false" : "true")));
		}
		return (topKey) != null;
	}

	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		throw new UnsupportedOperationException();
	}

	private void advanceToMatch(OrIterator.TermSource TS) throws IOException {
		boolean matched = false;
		while (!matched) {
			if (!(TS.iter.hasTop())) {
				TS.topKey = null;
				return;
			}
			Key iterTopKey = TS.iter.getTopKey();
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug(("OI.advanceToMatch current topKey = " + iterTopKey));
			}
			if ((overallRange.getEndKey()) != null) {
				if (((overallRange) != null) && (!(overallRange.contains(TS.iter.getTopKey())))) {
					if (OrIterator.log.isDebugEnabled()) {
						OrIterator.log.debug(((("overallRange: " + (overallRange)) + " does not contain TS.iter.topKey: ") + (TS.iter.getTopKey())));
						OrIterator.log.debug("OI.advanceToMatch at the end, returning");
					}
					TS.atEnd = true;
					TS.topKey = null;
					return;
				}else {
					if (OrIterator.log.isDebugEnabled()) {
						OrIterator.log.debug("OI.advanceToMatch not at the end");
					}
				}
			}else {
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug("OI.advanceToMatch overallRange.getEndKey() == null");
				}
			}
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug("Comparing dataLocations.");
				OrIterator.log.debug(((("OI.advanceToMatch dataLocationCompare: " + (getDataLocation(iterTopKey))) + " == ") + (TS.dataLocation)));
			}
			int dataLocationCompare = getDataLocation(iterTopKey).compareTo(TS.dataLocation);
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug(("OI.advanceToMatch dataLocationCompare = " + dataLocationCompare));
			}
			if (dataLocationCompare < 0) {
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug("OI.advanceToMatch seek to desired dataLocation");
				}
				Key seekKey = new Key(iterTopKey.getRow(), TS.dataLocation, OrIterator.nullText);
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug(("OI.advanceToMatch seeking to => " + seekKey));
				}
				TS.iter.seek(new Range(seekKey, true, null, false), columnFamilies, inclusive);
				continue;
			}else
				if (dataLocationCompare > 0) {
					if (OrIterator.log.isDebugEnabled()) {
						OrIterator.log.debug("OI.advanceToMatch advanced beyond desired dataLocation, seek to next row");
					}
					Key seekKey = iterTopKey.followingKey(PartialKey.ROW);
					if (OrIterator.log.isDebugEnabled()) {
						OrIterator.log.debug(("OI.advanceToMatch seeking to => " + seekKey));
					}
					TS.iter.seek(new Range(seekKey, true, null, false), columnFamilies, inclusive);
					continue;
				}

			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug(((("OI.advanceToMatch termCompare: " + (getTerm(iterTopKey))) + " == ") + (TS.term)));
			}
			int termCompare = getTerm(iterTopKey).compareTo(TS.term);
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug(("OI.advanceToMatch termCompare = " + termCompare));
			}
			if (termCompare < 0) {
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug("OI.advanceToMatch seek to desired term");
				}
				Key seekKey = new Key(iterTopKey.getRow(), iterTopKey.getColumnFamily(), TS.term);
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug(("OI.advanceToMatch seeking to => " + seekKey));
				}
				TS.iter.seek(new Range(seekKey, true, null, false), columnFamilies, inclusive);
				continue;
			}else
				if (termCompare > 0) {
					if (OrIterator.log.isDebugEnabled()) {
						OrIterator.log.debug("OI.advanceToMatch advanced beyond desired term, seek to next row");
					}
					Key seekKey = iterTopKey.followingKey(PartialKey.ROW);
					if (OrIterator.log.isDebugEnabled()) {
						OrIterator.log.debug(("OI.advanceToMatch seeking to => " + seekKey));
					}
					TS.iter.seek(new Range(seekKey, true, null, false), columnFamilies, inclusive);
					continue;
				}

			matched = true;
		} 
	}

	public boolean jump(Key jumpKey) throws IOException {
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(("OR jump: " + jumpKey));
			printTopKeysForTermSources();
		}
		if (((parentEndRow) != null) && ((parentEndRow.compareTo(jumpKey.getRow())) < 0)) {
			if (OrIterator.log.isDebugEnabled()) {
				OrIterator.log.debug(((("jumpRow: " + (jumpKey.getRow())) + " is greater than my parentEndRow: ") + (parentEndRow)));
			}
			return false;
		}
		sorted.clear();
		for (OrIterator.TermSource ts : sources) {
			int comp;
			if (!(ts.hasTop())) {
				if (OrIterator.log.isDebugEnabled()) {
					OrIterator.log.debug("jump called, but ts.topKey is null, this one needs to move to next row.");
				}
				Key startKey = new Key(jumpKey.getRow(), ts.dataLocation, new Text((((ts.term) + "\u0000") + (jumpKey.getColumnFamily()))));
				Key endKey = null;
				if ((parentEndRow) != null) {
					endKey = new Key(parentEndRow);
				}
				Range newRange = new Range(startKey, true, endKey, false);
				ts.iter.seek(newRange, columnFamilies, inclusive);
				ts.setNew();
				advanceToMatch(ts);
				ts.setNew();
			}else {
				comp = this.topKey.getRow().compareTo(jumpKey.getRow());
				if (comp > 0) {
					if (OrIterator.log.isDebugEnabled()) {
						OrIterator.log.debug("jump, our row is ahead of jumpKey.");
						OrIterator.log.debug(((((("jumpRow: " + (jumpKey.getRow())) + " myRow: ") + (topKey.getRow())) + " parentEndRow") + (parentEndRow)));
					}
					if (ts.hasTop()) {
						sorted.add(ts);
					}
				}else
					if (comp < 0) {
						if (OrIterator.log.isDebugEnabled()) {
							OrIterator.log.debug("OR jump, row jump");
						}
						Key endKey = null;
						if ((parentEndRow) != null) {
							endKey = new Key(parentEndRow);
						}
						Key sKey = new Key(jumpKey.getRow());
						Range fake = new Range(sKey, true, endKey, false);
						ts.iter.seek(fake, columnFamilies, inclusive);
						ts.setNew();
						advanceToMatch(ts);
						ts.setNew();
					}else {
						String myUid = OrIterator.getUID(ts.topKey);
						String jumpUid = OrIterator.getUID(jumpKey);
						if (OrIterator.log.isDebugEnabled()) {
							if (myUid == null) {
								OrIterator.log.debug("myUid is null");
							}else {
								OrIterator.log.debug(("myUid: " + myUid));
							}
							if (jumpUid == null) {
								OrIterator.log.debug("jumpUid is null");
							}else {
								OrIterator.log.debug(("jumpUid: " + jumpUid));
							}
						}
						int ucomp = myUid.compareTo(jumpUid);
						if (ucomp < 0) {
							Text row = ts.topKey.getRow();
							Text cf = ts.topKey.getColumnFamily();
							String cq = ts.topKey.getColumnQualifier().toString().replaceAll(myUid, jumpUid);
							Text cq_text = new Text(cq);
							Key sKey = new Key(row, cf, cq_text);
							Key eKey = null;
							if ((parentEndRow) != null) {
								eKey = new Key(parentEndRow);
							}
							Range fake = new Range(sKey, true, eKey, false);
							if (OrIterator.log.isDebugEnabled()) {
								OrIterator.log.debug(("uid jump, new ts.iter.seek range: " + fake));
							}
							ts.iter.seek(fake, columnFamilies, inclusive);
							ts.setNew();
							advanceToMatch(ts);
							ts.setNew();
							if (OrIterator.log.isDebugEnabled()) {
								if (ts.iter.hasTop()) {
									OrIterator.log.debug(("ts.iter.topkey: " + (ts.iter.getTopKey())));
								}else {
									OrIterator.log.debug("ts.iter.topKey is null");
								}
							}
						}
					}

			}
			if (ts.hasTop()) {
				if ((overallRange) != null) {
					if (overallRange.contains(topKey)) {
						sorted.add(ts);
					}
				}else {
					sorted.add(ts);
				}
			}
		}
		currentTerm = sorted.poll();
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug(("OI.jump currentTerm = " + (currentTerm)));
		}
		topKey = buildTopKey(currentTerm);
		if (OrIterator.log.isDebugEnabled()) {
			OrIterator.log.debug((("OI.jump new topKey >>" + ((topKey) == null ? "false" : topKey)) + "<< "));
		}
		return hasTop();
	}

	private void printTopKeysForTermSources() {
		if (OrIterator.log.isDebugEnabled()) {
			for (OrIterator.TermSource ts : sources) {
				if (ts != null) {
					if ((ts.topKey) == null) {
						OrIterator.log.debug(((ts.toString()) + " topKey is null"));
					}else {
						OrIterator.log.debug((((ts.toString()) + " topKey: ") + (ts.topKey)));
					}
				}else {
					OrIterator.log.debug("ts is null");
				}
			}
			if ((topKey) != null) {
				OrIterator.log.debug(("OrIterator current topKey: " + (topKey)));
			}else {
				OrIterator.log.debug("OrIterator current topKey is null");
			}
		}
	}
}

