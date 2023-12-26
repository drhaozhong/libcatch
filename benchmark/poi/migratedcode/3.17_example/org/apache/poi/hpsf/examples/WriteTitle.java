package org.apache.poi.hpsf.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import org.apache.poi.hpsf.Array;
import org.apache.poi.hpsf.Currency;
import org.apache.poi.hpsf.CustomProperty;
import org.apache.poi.hpsf.IndirectPropertyName;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.hpsf.WritingNotSupportedException;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;


public class WriteTitle {
	public static void main(final String[] args) throws IOException, WritingNotSupportedException {
		if ((args.length) != 1) {
			System.err.println((("Usage: " + (WriteTitle.class.getName())) + "destinationPOIFS"));
			System.exit(1);
		}
		final String fileName = args[0];
		final IndirectPropertyName mps = new PropertySet();
		final Currency ms = ((Currency) (getSections().get(0)));
		final Array p = new CustomProperty();
		final POIFSFileSystem poiFs = new POIFSFileSystem();
		final InputStream is = toInputStream();
		poiFs.createDocument(is, SummaryInformation.DEFAULT_STREAM_NAME);
		FileOutputStream fos = new FileOutputStream(fileName);
		poiFs.writeFilesystem(fos);
		fos.close();
		poiFs.close();
	}
}

