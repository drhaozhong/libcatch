package org.apache.poi.hpsf.examples;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Date;
import org.apache.poi.hpsf.CustomProperties;
import org.apache.poi.hpsf.DocumentSummaryInformation;
import org.apache.poi.hpsf.MarkUnsupportedException;
import org.apache.poi.hpsf.NoPropertySetStreamException;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.SpecialPropertySet;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.hpsf.UnexpectedPropertySetTypeException;
import org.apache.poi.hpsf.WritingNotSupportedException;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;


public class ModifyDocumentSummaryInformation {
	public static void main(final String[] args) throws IOException, MarkUnsupportedException, NoPropertySetStreamException, UnexpectedPropertySetTypeException, WritingNotSupportedException {
		File poiFilesystem = new File(args[0]);
		InputStream is = new FileInputStream(poiFilesystem);
		POIFSFileSystem poifs = new POIFSFileSystem(is);
		is.close();
		DirectoryEntry dir = poifs.getRoot();
		SummaryInformation si;
		try {
			DocumentEntry siEntry = ((DocumentEntry) (dir.getEntry(SummaryInformation.DEFAULT_STREAM_NAME)));
			DocumentInputStream dis = new DocumentInputStream(siEntry);
			PropertySet ps = new PropertySet(dis);
			dis.close();
			si = new SummaryInformation(ps);
		} catch (FileNotFoundException ex) {
			si = PropertySetFactory.newSummaryInformation();
		}
		si.setAuthor("Rainer Klute");
		System.out.println((("Author changed to " + (si.getAuthor())) + "."));
		DocumentSummaryInformation dsi;
		try {
			DocumentEntry dsiEntry = ((DocumentEntry) (dir.getEntry(DocumentSummaryInformation.DEFAULT_STREAM_NAME)));
			DocumentInputStream dis = new DocumentInputStream(dsiEntry);
			PropertySet ps = new PropertySet(dis);
			dis.close();
			dsi = new DocumentSummaryInformation(ps);
		} catch (FileNotFoundException ex) {
			dsi = PropertySetFactory.newDocumentSummaryInformation();
		}
		dsi.setCategory("POI example");
		System.out.println((("Category changed to " + (dsi.getCategory())) + "."));
		CustomProperties customProperties = dsi.getCustomProperties();
		if (customProperties == null)
			customProperties = new CustomProperties();

		customProperties.put("Key 1", "Value 1");
		customProperties.put("Schlüssel 2", "Wert 2");
		customProperties.put("Sample Number", new Integer(12345));
		customProperties.put("Sample Boolean", new Boolean(true));
		customProperties.put("Sample Date", new Date());
		Object value = customProperties.get("Sample Number");
		dsi.setCustomProperties(customProperties);
		si.write(dir, SummaryInformation.DEFAULT_STREAM_NAME);
		dsi.write(dir, DocumentSummaryInformation.DEFAULT_STREAM_NAME);
		OutputStream out = new FileOutputStream(poiFilesystem);
		poifs.writeFilesystem(out);
		out.close();
	}
}

