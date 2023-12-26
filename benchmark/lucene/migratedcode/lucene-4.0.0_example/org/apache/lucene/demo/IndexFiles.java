package org.apache.lucene.demo;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;
import static org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE;
import static org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE_OR_APPEND;


public class IndexFiles {
	private IndexFiles() {
	}

	public static void main(String[] args) {
		String usage = "java org.apache.lucene.demo.IndexFiles" + ((" [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n" + "This indexes the documents in DOCS_PATH, creating a Lucene index") + "in INDEX_PATH that can be searched with SearchFiles");
		String indexPath = "index";
		String docsPath = null;
		boolean create = true;
		for (int i = 0; i < (args.length); i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[(i + 1)];
				i++;
			}else
				if ("-docs".equals(args[i])) {
					docsPath = args[(i + 1)];
					i++;
				}else
					if ("-update".equals(args[i])) {
						create = false;
					}


		}
		if (docsPath == null) {
			System.err.println(("Usage: " + usage));
			System.exit(1);
		}
		final File docDir = new File(docsPath);
		if ((!(docDir.exists())) || (!(docDir.canRead()))) {
			System.out.println((("Document directory '" + (docDir.getAbsolutePath())) + "' does not exist or is not readable, please check the path"));
			System.exit(1);
		}
		Date start = new Date();
		try {
			System.out.println((("Indexing to directory '" + indexPath) + "'..."));
			Directory dir = FSDirectory.open(new File(indexPath));
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);
			if (create) {
				iwc.setOpenMode(CREATE);
			}else {
				iwc.setOpenMode(CREATE_OR_APPEND);
			}
			IndexWriter writer = new IndexWriter(dir, iwc);
			IndexFiles.indexDocs(writer, docDir);
			writer.close();
			Date end = new Date();
			System.out.println((((end.getTime()) - (start.getTime())) + " total milliseconds"));
		} catch (IOException e) {
			System.out.println((((" caught a " + (e.getClass())) + "\n with message: ") + (e.getMessage())));
		}
	}

	static void indexDocs(IndexWriter writer, File file) throws IOException {
		if (file.canRead()) {
			if (file.isDirectory()) {
				String[] files = file.list();
				if (files != null) {
					for (int i = 0; i < (files.length); i++) {
						IndexFiles.indexDocs(writer, new File(file, files[i]));
					}
				}
			}else {
				FileInputStream fis;
				try {
					fis = new FileInputStream(file);
				} catch (FileNotFoundException fnfe) {
					return;
				}
				try {
					Document doc = new Document();
					Field pathField = new StringField("path", file.getPath(), YES);
					doc.add(pathField);
					doc.add(new LongField("modified", file.lastModified(), NO));
					if ((writer.getConfig().getOpenMode()) == (CREATE)) {
						System.out.println(("adding " + file));
						writer.addDocument(doc);
					}else {
						System.out.println(("updating " + file));
						writer.updateDocument(new Term("path", file.getPath()), doc);
					}
				} finally {
					fis.close();
				}
			}
		}
	}
}

