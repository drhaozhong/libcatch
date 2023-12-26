package org.apache.lucene.demo;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.demo.html.HTMLParser;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import static org.apache.lucene.document.DateTools.Resolution.MINUTE;
import static org.apache.lucene.document.DateTools.Resolution.SECOND;
import static org.apache.lucene.document.Field.Index.TOKENIZED;
import static org.apache.lucene.document.Field.Index.UN_TOKENIZED;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;


public class HTMLDocument {
	static char dirSep = System.getProperty("file.separator").charAt(0);

	public static String uid(File f) {
		return ((f.getPath().replace(HTMLDocument.dirSep, '\u0000')) + "\u0000") + (DateTools.timeToString(f.lastModified(), SECOND));
	}

	public static String uid2url(String uid) {
		String url = uid.replace('\u0000', '/');
		return url.substring(0, url.lastIndexOf('/'));
	}

	public static Document Document(File f) throws IOException, InterruptedException {
		Document doc = new Document();
		doc.add(new Field("path", f.getPath().replace(HTMLDocument.dirSep, '/'), YES, UN_TOKENIZED));
		doc.add(new Field("modified", DateTools.timeToString(f.lastModified(), MINUTE), YES, UN_TOKENIZED));
		doc.add(new Field("uid", HTMLDocument.uid(f), NO, UN_TOKENIZED));
		FileInputStream fis = new FileInputStream(f);
		HTMLParser parser = new HTMLParser(fis);
		doc.add(new Field("contents", parser.getReader()));
		doc.add(new Field("summary", parser.getSummary(), YES, Field.Index.NO));
		doc.add(new Field("title", parser.getTitle(), YES, TOKENIZED));
		return doc;
	}

	private HTMLDocument() {
	}
}

