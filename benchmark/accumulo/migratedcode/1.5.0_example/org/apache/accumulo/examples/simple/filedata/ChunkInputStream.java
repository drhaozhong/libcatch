package org.apache.accumulo.examples.simple.filedata;


import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class ChunkInputStream extends InputStream {
	private static final Logger log = Logger.getLogger(ChunkInputStream.class);

	protected PeekingIterator<Map.Entry<Key, Value>> source;

	protected Key currentKey;

	protected Set<Text> currentVis;

	protected int currentChunk;

	protected int currentChunkSize;

	protected boolean gotEndMarker;

	protected byte[] buf;

	protected int count;

	protected int pos;

	public ChunkInputStream() {
		source = null;
	}

	public ChunkInputStream(PeekingIterator<Map.Entry<Key, Value>> in) throws IOException {
		setSource(in);
	}

	public void setSource(PeekingIterator<Map.Entry<Key, Value>> in) throws IOException {
		if ((source) != null)
			throw new IOException("setting new source without closing old one");

		this.source = in;
		currentVis = new TreeSet<Text>();
		count = pos = 0;
		if (!(source.hasNext())) {
			ChunkInputStream.log.debug("source has no next");
			gotEndMarker = true;
			return;
		}
		Map.Entry<Key, Value> entry = source.next();
		currentKey = entry.getKey();
		buf = entry.getValue().get();
		while (!(currentKey.getColumnFamily().equals(FileDataIngest.CHUNK_CF))) {
			ChunkInputStream.log.debug(("skipping key: " + (currentKey.toString())));
			if (!(source.hasNext()))
				return;

			entry = source.next();
			currentKey = entry.getKey();
			buf = entry.getValue().get();
		} 
		ChunkInputStream.log.debug(("starting chunk: " + (currentKey.toString())));
		count = buf.length;
		currentVis.add(currentKey.getColumnVisibility());
		currentChunk = FileDataIngest.bytesToInt(currentKey.getColumnQualifier().getBytes(), 4);
		currentChunkSize = FileDataIngest.bytesToInt(currentKey.getColumnQualifier().getBytes(), 0);
		gotEndMarker = false;
		if ((buf.length) == 0)
			gotEndMarker = true;

		if ((currentChunk) != 0) {
			source = null;
			throw new IOException(("starting chunk number isn't 0 for " + (currentKey.getRow())));
		}
	}

	private int fill() throws IOException {
		if (((source) == null) || (!(source.hasNext()))) {
			if (gotEndMarker)
				return count = pos = 0;
			else
				throw new IOException("no end chunk marker but source has no data");

		}
		Map.Entry<Key, Value> entry = source.peek();
		Key thisKey = entry.getKey();
		ChunkInputStream.log.debug(("evaluating key: " + (thisKey.toString())));
		if (!(thisKey.equals(currentKey, PartialKey.ROW))) {
			if (gotEndMarker)
				return -1;
			else {
				String currentRow = currentKey.getRow().toString();
				clear();
				throw new IOException(("got to the end of the row without end chunk marker " + currentRow));
			}
		}
		ChunkInputStream.log.debug("matches current key");
		source.next();
		if (!(thisKey.getColumnFamily().equals(FileDataIngest.CHUNK_CF))) {
			ChunkInputStream.log.debug("skipping non-chunk key");
			return fill();
		}
		ChunkInputStream.log.debug("is a chunk");
		if ((currentChunkSize) != (FileDataIngest.bytesToInt(thisKey.getColumnQualifier().getBytes(), 0))) {
			ChunkInputStream.log.debug("skipping chunk of different size");
			return fill();
		}
		if (!(currentVis.contains(thisKey.getColumnVisibility())))
			currentVis.add(thisKey.getColumnVisibility());

		if (thisKey.getColumnQualifier().equals(currentKey.getColumnQualifier())) {
			ChunkInputStream.log.debug("skipping identical chunk with different visibility");
			return fill();
		}
		if (gotEndMarker) {
			ChunkInputStream.log.debug(((("got another chunk after end marker: " + (currentKey.toString())) + " ") + (thisKey.toString())));
			clear();
			throw new IOException("found extra chunk after end marker");
		}
		int thisChunk = FileDataIngest.bytesToInt(thisKey.getColumnQualifier().getBytes(), 4);
		if (thisChunk != ((currentChunk) + 1)) {
			ChunkInputStream.log.debug(((("new chunk same file, unexpected chunkID: " + (currentKey.toString())) + " ") + (thisKey.toString())));
			clear();
			throw new IOException(((("missing chunks between " + (currentChunk)) + " and ") + thisChunk));
		}
		currentKey = thisKey;
		currentChunk = thisChunk;
		buf = entry.getValue().get();
		pos = 0;
		if ((buf.length) == 0) {
			gotEndMarker = true;
			return fill();
		}
		return count = buf.length;
	}

	public Set<Text> getVisibilities() {
		if ((source) != null)
			throw new IllegalStateException("don't get visibilities before chunks have been completely read");

		return currentVis;
	}

	public int read() throws IOException {
		if ((source) == null)
			return -1;

		ChunkInputStream.log.debug(((("pos: " + (pos)) + " count: ") + (count)));
		if ((pos) >= (count)) {
			if ((fill()) <= 0) {
				ChunkInputStream.log.debug(("done reading input stream at key: " + ((currentKey) == null ? "null" : currentKey.toString())));
				if (((source) != null) && (source.hasNext()))
					ChunkInputStream.log.debug(("next key: " + (source.peek().getKey())));

				clear();
				return -1;
			}
		}
		return (buf[((pos)++)]) & 255;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException();
		}else
			if (((((off < 0) || (off > (b.length))) || (len < 0)) || ((off + len) > (b.length))) || ((off + len) < 0)) {
				throw new IndexOutOfBoundsException();
			}else
				if (len == 0) {
					return 0;
				}


		ChunkInputStream.log.debug(((("filling buffer " + off) + " ") + len));
		int total = 0;
		while (total < len) {
			int avail = (count) - (pos);
			ChunkInputStream.log.debug((avail + " available in current local buffer"));
			if (avail <= 0) {
				if ((fill()) <= 0) {
					ChunkInputStream.log.debug(("done reading input stream at key: " + ((currentKey) == null ? "null" : currentKey.toString())));
					if (((source) != null) && (source.hasNext()))
						ChunkInputStream.log.debug(("next key: " + (source.peek().getKey())));

					clear();
					ChunkInputStream.log.debug((("filled " + total) + " bytes"));
					return total == 0 ? -1 : total;
				}
				avail = (count) - (pos);
			}
			int cnt = (avail < (len - total)) ? avail : len - total;
			ChunkInputStream.log.debug(((((("copying from local buffer: local pos " + (pos)) + " into pos ") + off) + " len ") + cnt));
			System.arraycopy(buf, pos, b, off, cnt);
			pos += cnt;
			off += cnt;
			total += cnt;
		} 
		ChunkInputStream.log.debug((("filled " + total) + " bytes"));
		return total;
	}

	public void clear() {
		source = null;
		buf = null;
		currentKey = null;
		currentChunk = 0;
		pos = count = 0;
	}

	@Override
	public void close() throws IOException {
		try {
			while ((fill()) > 0) {
			} 
		} catch (IOException e) {
			clear();
			throw new IOException(e);
		}
		clear();
	}
}

