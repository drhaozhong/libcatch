package org.apache.lucene.demo;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import static org.apache.lucene.document.DateTools.Resolution.MINUTE;
import static org.apache.lucene.document.Field.Index.UN_TOKENIZED;
import static org.apache.lucene.document.Field.Store.YES;


public class FileDocument {
	public static Document Document(File f) throws FileNotFoundException {
		Document doc = new Document();
		doc.add(new Field("path", f.getPath(), YES, UN_TOKENIZED));
		doc.add(new Field("modified", DateTools.timeToString(f.lastModified(), MINUTE), YES, UN_TOKENIZED));
		doc.add(new Field("contents", new FileReader(f)));
		return doc;
	}

	private FileDocument() {
	}
}

