package org.apache.poi.hpsf.examples;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import org.apache.poi.hpsf.CustomProperties;
import org.apache.poi.hpsf.DocumentSummaryInformation;
import org.apache.poi.hpsf.MarkUnsupportedException;
import org.apache.poi.hpsf.NoPropertySetStreamException;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.hpsf.UnexpectedPropertySetTypeException;
import org.apache.poi.hpsf.WritingNotSupportedException;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;


public class ModifyDocumentSummaryInformation {
	public static void main(final String[] args) throws IOException, MarkUnsupportedException, NoPropertySetStreamException, UnexpectedPropertySetTypeException, WritingNotSupportedException {
		File summaryFile = new File(args[0]);
		NPOIFSFileSystem poifs = new NPOIFSFileSystem(summaryFile, false);
		DirectoryEntry dir = poifs.getRoot();
		SummaryInformation si;
		try {
			si = ((SummaryInformation) (PropertySetFactory.create(dir, SummaryInformation.DEFAULT_STREAM_NAME)));
		} catch (FileNotFoundException ex) {
			si = PropertySetFactory.newSummaryInformation();
		}
		si.setAuthor("Rainer Klute");
		System.out.println((("Author changed to " + (si.getAuthor())) + "."));
		DocumentSummaryInformation dsi;
		try {
			dsi = ((DocumentSummaryInformation) (PropertySetFactory.create(dir, DocumentSummaryInformation.DEFAULT_STREAM_NAME)));
		} catch (FileNotFoundException ex) {
			dsi = PropertySetFactory.newDocumentSummaryInformation();
		}
		dsi.setCategory("POI example");
		System.out.println((("Category changed to " + (dsi.getCategory())) + "."));
		CustomProperties customProperties = dsi.getCustomProperties();
		if (customProperties == null)
			customProperties = new CustomProperties();

		customProperties.put("Key 1", "Value 1");
		customProperties.put("Schl\u00fcssel 2", "Wert 2");
		customProperties.put("Sample Number", new Integer(12345));
		customProperties.put("Sample Boolean", Boolean.TRUE);
		customProperties.put("Sample Date", new Date());
		Object value = customProperties.get("Sample Number");
		System.out.println(("Custom Sample Number is now " + value));
		dsi.setCustomProperties(customProperties);
		si.write(dir, SummaryInformation.DEFAULT_STREAM_NAME);
		dsi.write(dir, DocumentSummaryInformation.DEFAULT_STREAM_NAME);
		poifs.writeFilesystem();
		poifs.close();
	}
}

