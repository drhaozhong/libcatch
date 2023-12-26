package org.apache.poi.hpsf.examples;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;


public final class ReadTitle {
	private ReadTitle() {
	}

	public static void main(final String[] args) throws IOException {
		final String filename = args[0];
		POIFSReader r = new POIFSReader();
		r.registerListener(new ReadTitle.MyPOIFSReaderListener(), SummaryInformation.DEFAULT_STREAM_NAME);
		r.read(new File(filename));
	}

	static class MyPOIFSReaderListener implements POIFSReaderListener {
		@Override
		public void processPOIFSReaderEvent(final POIFSReaderEvent event) {
			SummaryInformation si;
			try {
				si = ((SummaryInformation) (PropertySetFactory.create(event.getStream())));
			} catch (Exception ex) {
				throw new RuntimeException((((("Property set stream \"" + (event.getPath())) + (event.getName())) + "\": ") + ex));
			}
			final String title = si.getTitle();
			if (title != null)
				System.out.println((("Title: \"" + title) + "\""));
			else
				System.out.println("Document has no title.");

		}
	}
}

