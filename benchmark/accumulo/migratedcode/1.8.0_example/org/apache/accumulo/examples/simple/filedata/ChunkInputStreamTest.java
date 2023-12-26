package org.apache.accumulo.examples.simple.filedata;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChunkInputStreamTest {
	private static final Logger log = LoggerFactory.getLogger(ChunkInputStream.class);

	private List<Map.Entry<Key, Value>> data;

	private List<Map.Entry<Key, Value>> baddata;

	private List<Map.Entry<Key, Value>> multidata;

	@org.junit.Before
	public void setupData() {
		data = new ArrayList<>();
		ChunkInputStreamTest.addData(data, "a", "refs", "id\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(data, "a", "refs", "id\u0000name", "A&B", "name");
		ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(data, "b", "refs", "id\u0000ext", "A&B", "ext");
		ChunkInputStreamTest.addData(data, "b", "refs", "id\u0000name", "A&B", "name");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "B&C", "");
		ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "D", "");
		ChunkInputStreamTest.addData(data, "c", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(data, "c", "~chunk", 100, 1, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(data, "c", "~chunk", 100, 2, "A&B", "");
		ChunkInputStreamTest.addData(data, "d", "~chunk", 100, 0, "A&B", "");
		ChunkInputStreamTest.addData(data, "e", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(data, "e", "~chunk", 100, 1, "A&B", "");
		baddata = new ArrayList<>();
		ChunkInputStreamTest.addData(baddata, "a", "~chunk", 100, 0, "A", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "b", "~chunk", 100, 0, "B", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "b", "~chunk", 100, 2, "C", "");
		ChunkInputStreamTest.addData(baddata, "c", "~chunk", 100, 0, "D", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "c", "~chunk", 100, 2, "E", "");
		ChunkInputStreamTest.addData(baddata, "d", "~chunk", 100, 0, "F", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "d", "~chunk", 100, 1, "G", "");
		ChunkInputStreamTest.addData(baddata, "d", "~zzzzz", "colq", "H", "");
		ChunkInputStreamTest.addData(baddata, "e", "~chunk", 100, 0, "I", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "e", "~chunk", 100, 1, "J", "");
		ChunkInputStreamTest.addData(baddata, "e", "~chunk", 100, 2, "I", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "f", "~chunk", 100, 2, "K", "asdfjkl;");
		ChunkInputStreamTest.addData(baddata, "g", "~chunk", 100, 0, "L", "");
		multidata = new ArrayList<>();
		ChunkInputStreamTest.addData(multidata, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "a", "~chunk", 100, 1, "A&B", "");
		ChunkInputStreamTest.addData(multidata, "a", "~chunk", 200, 0, "B&C", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "b", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "b", "~chunk", 200, 0, "B&C", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "b", "~chunk", 200, 1, "B&C", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "c", "~chunk", 100, 0, "A&B", "asdfjkl;");
		ChunkInputStreamTest.addData(multidata, "c", "~chunk", 100, 1, "B&C", "");
	}

	private static void addData(List<Map.Entry<Key, Value>> data, String row, String cf, String cq, String vis, String value) {
		data.add(new KeyValue(new Key(new Text(row), new Text(cf), new Text(cq), new Text(vis)), value.getBytes()));
	}

	private static void addData(List<Map.Entry<Key, Value>> data, String row, String cf, int chunkSize, int chunkCount, String vis, String value) {
		Text chunkCQ = new Text(FileDataIngest.intToBytes(chunkSize));
		chunkCQ.append(FileDataIngest.intToBytes(chunkCount), 0, 4);
		data.add(new KeyValue(new Key(new Text(row), new Text(cf), chunkCQ, new Text(vis)), value.getBytes()));
	}

	@org.junit.Test
	public void testExceptionOnMultipleSetSourceWithoutClose() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(data.iterator());
		pi = new PeekingIterator<>(data.iterator());
		cis.setSource(pi);
		try {
			cis.setSource(pi);
			Assert.fail();
		} catch (IOException e) {
		}
		cis.close();
	}

	@org.junit.Test
	public void testExceptionOnGetVisBeforeClose() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(data.iterator());
		cis.setSource(pi);
		try {
			cis.getVisibilities();
			Assert.fail();
		} catch (RuntimeException e) {
		}
		cis.close();
		cis.getVisibilities();
	}

	@org.junit.Test
	public void testReadIntoBufferSmallerThanChunks() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[5];
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(data.iterator());
		cis.setSource(pi);
		int read;
		Assert.assertEquals((read = cis.read(b)), 5);
		Assert.assertEquals(new String(b, 0, read), "asdfj");
		Assert.assertEquals((read = cis.read(b)), 3);
		Assert.assertEquals(new String(b, 0, read), "kl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 5);
		Assert.assertEquals(new String(b, 0, read), "qwert");
		Assert.assertEquals((read = cis.read(b)), 5);
		Assert.assertEquals(new String(b, 0, read), "yuiop");
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B, B&C, D]");
		cis.close();
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 5);
		Assert.assertEquals(new String(b, 0, read), "asdfj");
		Assert.assertEquals((read = cis.read(b)), 5);
		Assert.assertEquals(new String(b, 0, read), "kl;as");
		Assert.assertEquals((read = cis.read(b)), 5);
		Assert.assertEquals(new String(b, 0, read), "dfjkl");
		Assert.assertEquals((read = cis.read(b)), 1);
		Assert.assertEquals(new String(b, 0, read), ";");
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.close();
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 5);
		Assert.assertEquals(new String(b, 0, read), "asdfj");
		Assert.assertEquals((read = cis.read(b)), 3);
		Assert.assertEquals(new String(b, 0, read), "kl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		Assert.assertFalse(pi.hasNext());
	}

	@org.junit.Test
	public void testReadIntoBufferLargerThanChunks() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(data.iterator());
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 8);
		Assert.assertEquals(new String(b, 0, read), "asdfjkl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 10);
		Assert.assertEquals(new String(b, 0, read), "qwertyuiop");
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B, B&C, D]");
		cis.close();
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 16);
		Assert.assertEquals(new String(b, 0, read), "asdfjkl;asdfjkl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.close();
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 8);
		Assert.assertEquals(new String(b, 0, read), "asdfjkl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		Assert.assertFalse(pi.hasNext());
	}

	private static void assumeExceptionOnRead(ChunkInputStream cis, byte[] b) {
		try {
			Assert.assertEquals(0, cis.read(b));
			Assert.fail();
		} catch (IOException e) {
			ChunkInputStreamTest.log.debug("EXCEPTION {}", e.getMessage());
		}
	}

	private static void assumeExceptionOnClose(ChunkInputStream cis) {
		try {
			cis.close();
			Assert.fail();
		} catch (IOException e) {
			ChunkInputStreamTest.log.debug("EXCEPTION {}", e.getMessage());
		}
	}

	@org.junit.Test
	public void testBadData() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(baddata.iterator());
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
		Assert.assertEquals(cis.getVisibilities().toString(), "[A]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
		Assert.assertEquals(cis.getVisibilities().toString(), "[B, C]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
		Assert.assertEquals(cis.getVisibilities().toString(), "[D, E]");
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 8);
		Assert.assertEquals(new String(b, 0, read), "asdfjkl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[F, G]");
		cis.close();
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		cis.close();
		Assert.assertEquals(cis.getVisibilities().toString(), "[I, J]");
		try {
			cis.setSource(pi);
			Assert.fail();
		} catch (IOException e) {
		}
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
		Assert.assertEquals(cis.getVisibilities().toString(), "[K]");
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[L]");
		cis.close();
		Assert.assertFalse(pi.hasNext());
		pi = new PeekingIterator<>(baddata.iterator());
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
	}

	@org.junit.Test
	public void testBadDataWithoutClosing() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(baddata.iterator());
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		Assert.assertEquals(cis.getVisibilities().toString(), "[A]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		Assert.assertEquals(cis.getVisibilities().toString(), "[B, C]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		Assert.assertEquals(cis.getVisibilities().toString(), "[D, E]");
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 8);
		Assert.assertEquals(new String(b, 0, read), "asdfjkl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[F, G]");
		cis.close();
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		Assert.assertEquals(cis.getVisibilities().toString(), "[I, J]");
		try {
			cis.setSource(pi);
			Assert.fail();
		} catch (IOException e) {
		}
		Assert.assertEquals(cis.getVisibilities().toString(), "[K]");
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), (-1));
		Assert.assertEquals(cis.getVisibilities().toString(), "[L]");
		cis.close();
		Assert.assertFalse(pi.hasNext());
		pi = new PeekingIterator<>(baddata.iterator());
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnClose(cis);
	}

	@org.junit.Test
	public void testMultipleChunkSizes() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		byte[] b = new byte[20];
		int read;
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(multidata.iterator());
		b = new byte[20];
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 8);
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.setSource(pi);
		ChunkInputStreamTest.assumeExceptionOnRead(cis, b);
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B]");
		cis.setSource(pi);
		Assert.assertEquals((read = cis.read(b)), 8);
		Assert.assertEquals(new String(b, 0, read), "asdfjkl;");
		Assert.assertEquals((read = cis.read(b)), (-1));
		cis.close();
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B, B&C]");
		Assert.assertFalse(pi.hasNext());
	}

	@org.junit.Test
	public void testSingleByteRead() throws IOException {
		ChunkInputStream cis = new ChunkInputStream();
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(data.iterator());
		cis.setSource(pi);
		Assert.assertEquals(((byte) ('a')), ((byte) (cis.read())));
		Assert.assertEquals(((byte) ('s')), ((byte) (cis.read())));
		Assert.assertEquals(((byte) ('d')), ((byte) (cis.read())));
		Assert.assertEquals(((byte) ('f')), ((byte) (cis.read())));
		Assert.assertEquals(((byte) ('j')), ((byte) (cis.read())));
		Assert.assertEquals(((byte) ('k')), ((byte) (cis.read())));
		Assert.assertEquals(((byte) ('l')), ((byte) (cis.read())));
		Assert.assertEquals(((byte) (';')), ((byte) (cis.read())));
		Assert.assertEquals(cis.read(), (-1));
		cis.close();
		Assert.assertEquals(cis.getVisibilities().toString(), "[A&B]");
	}
}

