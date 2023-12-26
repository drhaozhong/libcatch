package org.apache.poi.poifs.poibrowser;


import java.io.IOException;
import java.io.PrintStream;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;


public class DocumentDescriptor {
	String name;

	POIFSDocumentPath path;

	DocumentInputStream stream;

	int size;

	byte[] bytes;

	public DocumentDescriptor(final String name, final POIFSDocumentPath path, final DocumentInputStream stream, final int nrOfBytes) {
		this.name = name;
		this.path = path;
		this.stream = stream;
		try {
			size = stream.available();
			if (stream.markSupported()) {
				stream.mark(nrOfBytes);
				final byte[] b = new byte[nrOfBytes];
				final int read = stream.read(b, 0, Math.min(size, b.length));
				bytes = new byte[read];
				System.arraycopy(b, 0, bytes, 0, read);
				stream.reset();
			}
		} catch (IOException ex) {
			System.out.println(ex);
		}
	}
}

