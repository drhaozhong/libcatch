package org.apache.lucene.demo.html;


import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;


class ParserThread extends Thread {
	HTMLParser parser;

	ParserThread(HTMLParser p) {
		parser = p;
	}

	public void run() {
		try {
			try {
				parser.HTMLDocument();
			} catch (ParseException e) {
				System.out.println(("Parse Aborted: " + (e.getMessage())));
			} catch (TokenMgrError e) {
				System.out.println(("Parse Aborted: " + (e.getMessage())));
			} finally {
				parser.pipeOut.close();
				synchronized(parser) {
					parser.summary.setLength(HTMLParser.SUMMARY_LENGTH);
					parser.titleComplete = true;
					parser.notifyAll();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

