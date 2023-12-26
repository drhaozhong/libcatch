package org.apache.lucene.demo;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;


public class IndexFiles {
	private IndexFiles() {
	}

	static final File INDEX_DIR = new File("index");

	public static void main(String[] args) {
		String usage = "java org.apache.lucene.demo.IndexFiles <root_directory>";
		if ((args.length) == 0) {
			System.err.println(("Usage: " + usage));
			System.exit(1);
		}
		if (IndexFiles.INDEX_DIR.exists()) {
			System.out.println((("Cannot save index to '" + (IndexFiles.INDEX_DIR)) + "' directory, please delete it first"));
			System.exit(1);
		}
		final File docDir = new File(args[0]);
		if ((!(docDir.exists())) || (!(docDir.canRead()))) {
			System.out.println((("Document directory '" + (docDir.getAbsolutePath())) + "' does not exist or is not readable, please check the path"));
			System.exit(1);
		}
		Date start = new Date();
		try {
			IndexWriter writer = new IndexWriter(IndexFiles.INDEX_DIR, new StandardAnalyzer(), true);
			System.out.println((("Indexing to directory '" + (IndexFiles.INDEX_DIR)) + "'..."));
			IndexFiles.indexDocs(writer, docDir);
			System.out.println("Optimizing...");
			writer.optimize();
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
				System.out.println(("adding " + file));
				try {
					writer.addDocument(FileDocument.Document(file));
				} catch (FileNotFoundException fnfe) {
				}
			}
		}
	}
}

