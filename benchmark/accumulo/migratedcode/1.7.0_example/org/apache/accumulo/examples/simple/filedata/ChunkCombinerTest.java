package org.apache.accumulo.examples.simple.filedata;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import junit.framework.TestCase;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;


public class ChunkCombinerTest extends TestCase {
	public static class MapIterator implements SortedKeyValueIterator<Key, Value> {
		private Iterator<Map.Entry<Key, Value>> iter;

		private Map.Entry<Key, Value> entry;

		Collection<ByteSequence> columnFamilies;

		private SortedMap<Key, Value> map;

		private Range range;

		@Override
		public ChunkCombinerTest.MapIterator deepCopy(IteratorEnvironment env) {
			return new ChunkCombinerTest.MapIterator(map);
		}

		private MapIterator(SortedMap<Key, Value> map) {
			this.map = map;
			iter = map.entrySet().iterator();
			this.range = new Range();
			if (iter.hasNext())
				entry = iter.next();
			else
				entry = null;

		}

		@Override
		public Key getTopKey() {
			return entry.getKey();
		}

		@Override
		public Value getTopValue() {
			return entry.getValue();
		}

		@Override
		public boolean hasTop() {
			return (entry) != null;
		}

		@Override
		public void next() throws IOException {
			entry = null;
			while (iter.hasNext()) {
				entry = iter.next();
				if (((columnFamilies.size()) > 0) && (!(columnFamilies.contains(entry.getKey().getColumnFamilyData())))) {
					entry = null;
					continue;
				}
				if (range.afterEndKey(entry.getKey()))
					entry = null;

				break;
			} 
		}

		@Override
		public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
			if (!inclusive) {
				throw new IllegalArgumentException("can only do inclusive colf filtering");
			}
			this.columnFamilies = columnFamilies;
			this.range = range;
			Key key = range.getStartKey();
			if (key == null) {
				key = new Key();
			}
			iter = map.tailMap(key).entrySet().iterator();
			next();
			while ((hasTop()) && (range.beforeStartKey(getTopKey()))) {
				next();
			} 
		}

		@Override
		public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
			throw new UnsupportedOperationException();
		}
	}

	private TreeMap<Key, Value> row1;

	private TreeMap<Key, Value> row2;

	private TreeMap<Key, Value> row3;

	private TreeMap<Key, Value> allRows;

	private TreeMap<Key, Value> cRow1;

	private TreeMap<Key, Value> cRow2;

	private TreeMap<Key, Value> cRow3;

	private TreeMap<Key, Value> allCRows;

	private TreeMap<Key, Value> cOnlyRow1;

	private TreeMap<Key, Value> cOnlyRow2;

	private TreeMap<Key, Value> cOnlyRow3;

	private TreeMap<Key, Value> allCOnlyRows;

	private TreeMap<Key, Value> badrow;

	@Override
	protected void setUp() {
		row1 = new TreeMap<Key, Value>();
		row2 = new TreeMap<Key, Value>();
		row3 = new TreeMap<Key, Value>();
		allRows = new TreeMap<Key, Value>();
		cRow1 = new TreeMap<Key, Value>();
		cRow2 = new TreeMap<Key, Value>();
		cRow3 = new TreeMap<Key, Value>();
		allCRows = new TreeMap<Key, Value>();
		cOnlyRow1 = new TreeMap<Key, Value>();
		cOnlyRow2 = new TreeMap<Key, Value>();
		cOnlyRow3 = new TreeMap<Key, Value>();
		allCOnlyRows = new TreeMap<Key, Value>();
		badrow = new TreeMap<Key, Value>();
		String refs = FileDataIngest.REFS_CF.toString();
		String fileext = FileDataIngest.REFS_FILE_EXT;
		String filename = FileDataIngest.REFS_ORIG_FILE;
		String chunk_cf = FileDataIngest.CHUNK_CF.toString();
		row1.put(new Key("row1", refs, ("hash1\u0000" + fileext), "C"), new Value("jpg".getBytes()));
		row1.put(new Key("row1", refs, ("hash1\u0000" + filename), "D"), new Value("foo1.jpg".getBytes()));
		row1.put(new Key("row1", chunk_cf, "0000", "A"), new Value("V1".getBytes()));
		row1.put(new Key("row1", chunk_cf, "0000", "B"), new Value("V1".getBytes()));
		row1.put(new Key("row1", chunk_cf, "0001", "A"), new Value("V2".getBytes()));
		row1.put(new Key("row1", chunk_cf, "0001", "B"), new Value("V2".getBytes()));
		cRow1.put(new Key("row1", refs, ("hash1\u0000" + fileext), "C"), new Value("jpg".getBytes()));
		cRow1.put(new Key("row1", refs, ("hash1\u0000" + filename), "D"), new Value("foo1.jpg".getBytes()));
		cRow1.put(new Key("row1", chunk_cf, "0000", "(C)|(D)"), new Value("V1".getBytes()));
		cRow1.put(new Key("row1", chunk_cf, "0001", "(C)|(D)"), new Value("V2".getBytes()));
		cOnlyRow1.put(new Key("row1", chunk_cf, "0000", "(C)|(D)"), new Value("V1".getBytes()));
		cOnlyRow1.put(new Key("row1", chunk_cf, "0001", "(C)|(D)"), new Value("V2".getBytes()));
		row2.put(new Key("row2", refs, ("hash1\u0000" + fileext), "A"), new Value("jpg".getBytes()));
		row2.put(new Key("row2", refs, ("hash1\u0000" + filename), "B"), new Value("foo1.jpg".getBytes()));
		row2.put(new Key("row2", chunk_cf, "0000", "A|B"), new Value("V1".getBytes()));
		row2.put(new Key("row2", chunk_cf, "0000", "A"), new Value("V1".getBytes()));
		row2.put(new Key("row2", chunk_cf, "0000", "(A)|(B)"), new Value("V1".getBytes()));
		row2.put(new Key("row2a", chunk_cf, "0000", "C"), new Value("V1".getBytes()));
		cRow2.put(new Key("row2", refs, ("hash1\u0000" + fileext), "A"), new Value("jpg".getBytes()));
		cRow2.put(new Key("row2", refs, ("hash1\u0000" + filename), "B"), new Value("foo1.jpg".getBytes()));
		cRow2.put(new Key("row2", chunk_cf, "0000", "(A)|(B)"), new Value("V1".getBytes()));
		cOnlyRow2.put(new Key("row2", chunk_cf, "0000", "(A)|(B)"), new Value("V1".getBytes()));
		row3.put(new Key("row3", refs, "hash1\u0000w", "(A&B)|(C&(D|E))"), new Value("".getBytes()));
		row3.put(new Key("row3", refs, "hash1\u0000x", "A&B"), new Value("".getBytes()));
		row3.put(new Key("row3", refs, "hash1\u0000y", "(A&B)"), new Value("".getBytes()));
		row3.put(new Key("row3", refs, "hash1\u0000z", "(F|G)&(D|E)"), new Value("".getBytes()));
		row3.put(new Key("row3", chunk_cf, "0000", "(A&B)|(C&(D|E))", 10), new Value("V1".getBytes()));
		row3.put(new Key("row3", chunk_cf, "0000", "A&B", 20), new Value("V1".getBytes()));
		row3.put(new Key("row3", chunk_cf, "0000", "(A&B)", 10), new Value("V1".getBytes()));
		row3.put(new Key("row3", chunk_cf, "0000", "(F|G)&(D|E)", 10), new Value("V1".getBytes()));
		cRow3.put(new Key("row3", refs, "hash1\u0000w", "(A&B)|(C&(D|E))"), new Value("".getBytes()));
		cRow3.put(new Key("row3", refs, "hash1\u0000x", "A&B"), new Value("".getBytes()));
		cRow3.put(new Key("row3", refs, "hash1\u0000y", "(A&B)"), new Value("".getBytes()));
		cRow3.put(new Key("row3", refs, "hash1\u0000z", "(F|G)&(D|E)"), new Value("".getBytes()));
		cRow3.put(new Key("row3", chunk_cf, "0000", "((F|G)&(D|E))|(A&B)|(C&(D|E))", 20), new Value("V1".getBytes()));
		cOnlyRow3.put(new Key("row3", chunk_cf, "0000", "((F|G)&(D|E))|(A&B)|(C&(D|E))", 20), new Value("V1".getBytes()));
		badrow.put(new Key("row1", chunk_cf, "0000", "A"), new Value("V1".getBytes()));
		badrow.put(new Key("row1", chunk_cf, "0000", "B"), new Value("V2".getBytes()));
		allRows.putAll(row1);
		allRows.putAll(row2);
		allRows.putAll(row3);
		allCRows.putAll(cRow1);
		allCRows.putAll(cRow2);
		allCRows.putAll(cRow3);
		allCOnlyRows.putAll(cOnlyRow1);
		allCOnlyRows.putAll(cOnlyRow2);
		allCOnlyRows.putAll(cOnlyRow3);
	}

	private static final Collection<ByteSequence> emptyColfs = new HashSet<ByteSequence>();

	public void test1() throws IOException {
		runTest(false, allRows, allCRows, ChunkCombinerTest.emptyColfs);
		runTest(true, allRows, allCRows, ChunkCombinerTest.emptyColfs);
		runTest(false, allRows, allCOnlyRows, Collections.singleton(FileDataIngest.CHUNK_CF_BS));
		runTest(true, allRows, allCOnlyRows, Collections.singleton(FileDataIngest.CHUNK_CF_BS));
		try {
			runTest(true, badrow, null, ChunkCombinerTest.emptyColfs);
			TestCase.assertNotNull(null);
		} catch (RuntimeException e) {
			TestCase.assertNull(null);
		}
	}

	private void runTest(boolean reseek, TreeMap<Key, Value> source, TreeMap<Key, Value> result, Collection<ByteSequence> cols) throws IOException {
		ChunkCombinerTest.MapIterator src = new ChunkCombinerTest.MapIterator(source);
		SortedKeyValueIterator<Key, Value> iter = new ChunkCombiner();
		iter.init(src, null, null);
		iter = iter.deepCopy(null);
		iter.seek(new Range(), cols, true);
		TreeMap<Key, Value> seen = new TreeMap<Key, Value>();
		while (iter.hasTop()) {
			TestCase.assertFalse(("already contains " + (iter.getTopKey())), seen.containsKey(iter.getTopKey()));
			seen.put(new Key(iter.getTopKey()), new Value(iter.getTopValue()));
			if (reseek)
				iter.seek(new Range(iter.getTopKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL), true, null, true), cols, true);
			else
				iter.next();

		} 
		TestCase.assertEquals(result, seen);
	}
}

