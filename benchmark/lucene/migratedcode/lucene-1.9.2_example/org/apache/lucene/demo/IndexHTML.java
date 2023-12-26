package org.apache.lucene.demo;


import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Date;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;


public class IndexHTML {
	private IndexHTML() {
	}

	private static boolean deleting = false;

	private static IndexReader reader;

	private static IndexWriter writer;

	private static TermEnum uidIter;

	public static void main(String[] argv) {
		try {
			String index = "index";
			boolean create = false;
			File root = null;
			String usage = "IndexHTML [-create] [-index <index>] <root_directory>";
			if ((argv.length) == 0) {
				System.err.println(("Usage: " + usage));
				return;
			}
			for (int i = 0; i < (argv.length); i++) {
				if (argv[i].equals("-index")) {
					index = argv[(++i)];
				}else
					if (argv[i].equals("-create")) {
						create = true;
					}else
						if (i != ((argv.length) - 1)) {
							System.err.println(("Usage: " + usage));
							return;
						}else
							root = new File(argv[i]);



			}
			Date start = new Date();
			if (!create) {
				IndexHTML.deleting = true;
				IndexHTML.indexDocs(root, index, create);
			}
			IndexHTML.writer = new IndexWriter(index, new StandardAnalyzer(), create);
			IndexHTML.writer.setMaxFieldLength(1000000);
			IndexHTML.indexDocs(root, index, create);
			System.out.println("Optimizing index...");
			IndexHTML.writer.optimize();
			IndexHTML.writer.close();
			Date end = new Date();
			System.out.print(((end.getTime()) - (start.getTime())));
			System.out.println(" total milliseconds");
		} catch (Exception e) {
			System.out.println((((" caught a " + (e.getClass())) + "\n with message: ") + (e.getMessage())));
		}
	}

	private static void indexDocs(File file, String index, boolean create) throws Exception {
		if (!create) {
			IndexHTML.reader = IndexReader.open(index);
			IndexHTML.uidIter = IndexHTML.reader.terms(new Term("uid", ""));
			IndexHTML.indexDocs(file);
			if (IndexHTML.deleting) {
				while (((IndexHTML.uidIter.term()) != null) && ((IndexHTML.uidIter.term().field()) == "uid")) {
					System.out.println(("deleting " + (HTMLDocument.uid2url(IndexHTML.uidIter.term().text()))));
					IndexHTML.reader.terms(IndexHTML.uidIter.term());
					IndexHTML.uidIter.next();
				} 
				IndexHTML.deleting = false;
			}
			IndexHTML.uidIter.close();
			IndexHTML.reader.close();
		}else
			IndexHTML.indexDocs(file);

	}

	private static void indexDocs(File file) throws Exception {
		if (file.isDirectory()) {
			String[] files = file.list();
			Arrays.sort(files);
			for (int i = 0; i < (files.length); i++)
				IndexHTML.indexDocs(new File(file, files[i]));

		}else
			if (((file.getPath().endsWith(".html")) || (file.getPath().endsWith(".htm"))) || (file.getPath().endsWith(".txt"))) {
				if ((IndexHTML.uidIter) != null) {
					String uid = HTMLDocument.uid(file);
					while ((((IndexHTML.uidIter.term()) != null) && ((IndexHTML.uidIter.term().field()) == "uid")) && ((IndexHTML.uidIter.term().text().compareTo(uid)) < 0)) {
						if (IndexHTML.deleting) {
							System.out.println(("deleting " + (HTMLDocument.uid2url(IndexHTML.uidIter.term().text()))));
							IndexHTML.reader.deleteDocuments(IndexHTML.uidIter.term());
						}
						IndexHTML.uidIter.next();
					} 
					if ((((IndexHTML.uidIter.term()) != null) && ((IndexHTML.uidIter.term().field()) == "uid")) && ((IndexHTML.uidIter.term().text().compareTo(uid)) == 0)) {
						IndexHTML.uidIter.next();
					}else
						if (!(IndexHTML.deleting)) {
							Document doc = HTMLDocument.Document(file);
							System.out.println(("adding " + (doc.get("path"))));
							IndexHTML.writer.addDocument(doc);
						}

				}else {
					Document doc = HTMLDocument.Document(file);
					System.out.println(("adding " + (doc.get("path"))));
					IndexHTML.writer.addDocument(doc);
				}
			}

	}
}

