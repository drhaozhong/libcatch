package org.apache.lucene.demo;


import java.io.PrintStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


public class DeleteFiles {
	private DeleteFiles() {
	}

	public static void main(String[] args) {
		String usage = "java org.apache.lucene.demo.DeleteFiles <unique_term>";
		if ((args.length) == 0) {
			System.err.println(("Usage: " + usage));
			System.exit(1);
		}
		try {
			Directory directory = FSDirectory.getDirectory("index", false);
			IndexReader reader = IndexReader.open(directory);
			Term term = new Term("path", args[0]);
			int deleted = reader.docFreq(term);
			System.out.println(((("deleted " + deleted) + " documents containing ") + term));
			reader.close();
			directory.close();
		} catch (Exception e) {
			System.out.println((((" caught a " + (e.getClass())) + "\n with message: ") + (e.getMessage())));
		}
	}
}

