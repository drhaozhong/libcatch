package org.apache.lucene.demo.html;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.Reader;
import java.util.Arrays;


class Test {
	public static void main(String[] argv) throws IOException, InterruptedException {
		if ("-dir".equals(argv[0])) {
			String[] files = new File(argv[1]).list();
			Arrays.sort(files);
			for (int i = 0; i < (files.length); i++) {
				System.err.println(files[i]);
				File file = new File(argv[1], files[i]);
				Test.parse(file);
			}
		}else
			Test.parse(new File(argv[0]));

	}

	public static void parse(File file) throws IOException, InterruptedException {
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(file);
			HTMLParser parser = new HTMLParser(fis);
			System.out.println(("Title: " + (Entities.encode(parser.getTitle()))));
			System.out.println(("Summary: " + (Entities.encode(parser.getSummary()))));
			System.out.println("Content:");
			LineNumberReader reader = new LineNumberReader(parser.getReader());
			for (String l = reader.readLine(); l != null; l = reader.readLine())
				System.out.println(l);

		} finally {
			if (fis != null)
				fis.close();

		}
	}
}

