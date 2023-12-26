package org.apache.poi.hpsf.examples;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.hpsf.ClassID;
import org.apache.poi.hpsf.NoPropertySetStreamException;
import org.apache.poi.hpsf.Property;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.Section;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;
import org.apache.poi.util.HexDump;


public final class ReadCustomPropertySets {
	private ReadCustomPropertySets() {
	}

	public static void main(final String[] args) throws IOException {
		final String filename = args[0];
		POIFSReader r = new POIFSReader();
		r.registerListener(new ReadCustomPropertySets.MyPOIFSReaderListener());
		r.read(new File(filename));
	}

	static class MyPOIFSReaderListener implements POIFSReaderListener {
		@Override
		public void processPOIFSReaderEvent(final POIFSReaderEvent event) {
			PropertySet ps;
			try {
				ps = PropertySetFactory.create(event.getStream());
			} catch (NoPropertySetStreamException ex) {
				ReadCustomPropertySets.out(((("No property set stream: \"" + (event.getPath())) + (event.getName())) + "\""));
				return;
			} catch (Exception ex) {
				throw new RuntimeException((((("Property set stream \"" + (event.getPath())) + (event.getName())) + "\": ") + ex));
			}
			ReadCustomPropertySets.out(((("Property set stream \"" + (event.getPath())) + (event.getName())) + "\":"));
			final long sectionCount = ps.getSectionCount();
			ReadCustomPropertySets.out(("   No. of sections: " + sectionCount));
			List<Section> sections = ps.getSections();
			int nr = 0;
			for (Section sec : sections) {
				ReadCustomPropertySets.out((("   Section " + (nr++)) + ":"));
				String s = ReadCustomPropertySets.hex(sec.getFormatID().getBytes());
				s = s.substring(0, ((s.length()) - 1));
				ReadCustomPropertySets.out(("      Format ID: " + s));
				int propertyCount = sec.getPropertyCount();
				ReadCustomPropertySets.out(("      No. of properties: " + propertyCount));
				Property[] properties = sec.getProperties();
				for (Property p : properties) {
					long id = p.getID();
					long type = p.getType();
					Object value = p.getValue();
					ReadCustomPropertySets.out(((((("      Property ID: " + id) + ", type: ") + type) + ", value: ") + value));
				}
			}
		}
	}

	private static void out(final String msg) {
		System.out.println(msg);
	}

	private static String hex(final byte[] bytes) {
		return HexDump.dump(bytes, 0L, 0);
	}
}

