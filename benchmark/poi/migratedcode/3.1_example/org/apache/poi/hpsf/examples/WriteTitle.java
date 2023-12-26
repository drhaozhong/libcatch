package org.apache.poi.hpsf.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.hpsf.MutableProperty;
import org.apache.poi.hpsf.MutablePropertySet;
import org.apache.poi.hpsf.MutableSection;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.hpsf.Variant;
import org.apache.poi.hpsf.WritingNotSupportedException;
import org.apache.poi.hpsf.wellknown.PropertyIDMap;
import org.apache.poi.hpsf.wellknown.SectionIDMap;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;


public class WriteTitle {
	public static void main(final String[] args) throws IOException, WritingNotSupportedException {
		if ((args.length) != 1) {
			System.err.println((("Usage: " + (WriteTitle.class.getName())) + "destinationPOIFS"));
			System.exit(1);
		}
		final String fileName = args[0];
		final MutablePropertySet mps = new MutablePropertySet();
		final MutableSection ms = ((MutableSection) (mps.getSections().get(0)));
		ms.setFormatID(SectionIDMap.SUMMARY_INFORMATION_ID);
		final MutableProperty p = new MutableProperty();
		p.setID(PropertyIDMap.PID_TITLE);
		p.setType(Variant.VT_LPWSTR);
		p.setValue("Sample title");
		ms.setProperty(p);
		final POIFSFileSystem poiFs = new POIFSFileSystem();
		final InputStream is = mps.toInputStream();
		poiFs.createDocument(is, SummaryInformation.DEFAULT_STREAM_NAME);
		poiFs.writeFilesystem(new FileOutputStream(fileName));
	}
}

