package org.apache.accumulo.examples.wikisearch.reader;


import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;


public class LfLineReader {
	private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

	private int bufferSize = LfLineReader.DEFAULT_BUFFER_SIZE;

	private InputStream in;

	private byte[] buffer;

	private int bufferLength = 0;

	private int bufferPosn = 0;

	private static final byte LF = '\n';

	public LfLineReader(InputStream in) {
		this(in, LfLineReader.DEFAULT_BUFFER_SIZE);
	}

	public LfLineReader(InputStream in, int bufferSize) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.buffer = new byte[this.bufferSize];
	}

	public LfLineReader(InputStream in, Configuration conf) throws IOException {
		this(in, conf.getInt("io.file.buffer.size", LfLineReader.DEFAULT_BUFFER_SIZE));
	}

	public void close() throws IOException {
		in.close();
	}

	public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
		str.clear();
		int txtLength = 0;
		int newlineLength = 0;
		long bytesConsumed = 0;
		do {
			int startPosn = bufferPosn;
			if ((bufferPosn) >= (bufferLength)) {
				startPosn = bufferPosn = 0;
				bufferLength = in.read(buffer);
				if ((bufferLength) <= 0)
					break;

			}
			for (; (bufferPosn) < (bufferLength); ++(bufferPosn)) {
				if ((buffer[bufferPosn]) == (LfLineReader.LF)) {
					newlineLength = 1;
					++(bufferPosn);
					break;
				}
			}
			int readLength = (bufferPosn) - startPosn;
			bytesConsumed += readLength;
			int appendLength = readLength - newlineLength;
			if (appendLength > (maxLineLength - txtLength)) {
				appendLength = maxLineLength - txtLength;
			}
			if (appendLength > 0) {
				str.append(buffer, startPosn, appendLength);
				txtLength += appendLength;
			}
		} while ((newlineLength == 0) && (bytesConsumed < maxBytesToConsume) );
		if (bytesConsumed > (Integer.MAX_VALUE))
			throw new IOException(("Too many bytes before newline: " + bytesConsumed));

		return ((int) (bytesConsumed));
	}

	public int readLine(Text str, int maxLineLength) throws IOException {
		return readLine(str, maxLineLength, Integer.MAX_VALUE);
	}

	public int readLine(Text str) throws IOException {
		return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
	}
}

