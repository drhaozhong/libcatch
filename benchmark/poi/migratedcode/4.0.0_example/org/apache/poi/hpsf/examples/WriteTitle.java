package org.apache.poi.hpsf.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.hpsf.ClassID;
import org.apache.poi.hpsf.Property;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.Section;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.hpsf.Variant;
import org.apache.poi.hpsf.WritingNotSupportedException;
import org.apache.poi.hpsf.wellknown.PropertyIDMap;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;


public class WriteTitle {
	public static void main(final String[] args) throws IOException, WritingNotSupportedException {
		if ((args.length) != 1) {
			System.err.println((("Usage: " + (WriteTitle.class.getName())) + "destinationPOIFS"));
			System.exit(1);
		}
		final String fileName = args[0];
		final PropertySet mps = new PropertySet();
		final Section ms = mps.getSections().get(0);
		ms.setFormatID(SummaryInformation.FORMAT_ID);
		final Property p = new Property();
		p.setID(PropertyIDMap.PID_TITLE);
		p.setType(Variant.VT_LPWSTR);
		p.setValue("Sample title");
		ms.setProperty(p);
		try (final POIFSFileSystem poiFs = new POIFSFileSystem()) {
			final InputStream is = mps.toInputStream();
			poiFs.createDocument(is, SummaryInformation.DEFAULT_STREAM_NAME);
			try (FileOutputStream fos = new FileOutputStream(fileName)) {
				poiFs.writeFilesystem(fos);
			}
		}
	}
}

