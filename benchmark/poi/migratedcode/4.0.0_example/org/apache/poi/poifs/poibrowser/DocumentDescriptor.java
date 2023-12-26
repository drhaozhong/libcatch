package org.apache.poi.poifs.poibrowser;


import java.io.IOException;
import java.io.PrintStream;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;
import org.apache.poi.util.IOUtils;


class DocumentDescriptor {
	private static final int MAX_RECORD_LENGTH = 100000;

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
			if (stream.markSupported()) {
				stream.mark(nrOfBytes);
				bytes = IOUtils.toByteArray(stream, nrOfBytes, DocumentDescriptor.MAX_RECORD_LENGTH);
				stream.reset();
			}else {
				bytes = new byte[0];
			}
			size = (bytes.length) + (stream.available());
		} catch (IOException ex) {
			System.out.println(ex);
		}
	}
}

