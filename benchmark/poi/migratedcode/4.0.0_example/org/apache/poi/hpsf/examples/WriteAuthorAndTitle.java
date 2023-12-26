package org.apache.poi.hpsf.examples;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.poi.hpsf.HPSFRuntimeException;
import org.apache.poi.hpsf.MarkUnsupportedException;
import org.apache.poi.hpsf.NoPropertySetStreamException;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.Section;
import org.apache.poi.hpsf.Variant;
import org.apache.poi.hpsf.WritingNotSupportedException;
import org.apache.poi.hpsf.wellknown.PropertyIDMap;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;


public final class WriteAuthorAndTitle {
	private WriteAuthorAndTitle() {
	}

	public static void main(final String[] args) throws IOException {
		if ((args.length) != 2) {
			System.err.println((("Usage: " + (WriteAuthorAndTitle.class.getName())) + " originPOIFS destinationPOIFS"));
			System.exit(1);
		}
		final String srcName = args[0];
		final String dstName = args[1];
		final POIFSReader r = new POIFSReader();
		final WriteAuthorAndTitle.ModifySICopyTheRest msrl = new WriteAuthorAndTitle.ModifySICopyTheRest(dstName);
		r.registerListener(msrl);
		r.read(new File(srcName));
		msrl.close();
	}

	static class ModifySICopyTheRest implements POIFSReaderListener {
		private String dstName;

		private OutputStream out;

		private POIFSFileSystem poiFs;

		ModifySICopyTheRest(final String dstName) {
			this.dstName = dstName;
			poiFs = new POIFSFileSystem();
		}

		@Override
		public void processPOIFSReaderEvent(final POIFSReaderEvent event) {
			final POIFSDocumentPath path = event.getPath();
			final String name = event.getName();
			final DocumentInputStream stream = event.getStream();
			Throwable t = null;
			try {
				if (PropertySet.isPropertySetStream(stream)) {
					try {
						PropertySet ps = PropertySetFactory.create(stream);
						if (ps.isSummaryInformation()) {
							editSI(poiFs, path, name, ps);
						}else {
							copy(poiFs, path, name, ps);
						}
					} catch (NoPropertySetStreamException ex) {
					}
				}else {
					copy(poiFs, event.getPath(), event.getName(), stream);
				}
			} catch (MarkUnsupportedException | WritingNotSupportedException | IOException ex) {
				t = ex;
			}
			if (t != null) {
				throw new HPSFRuntimeException(((("Could not read file \"" + path) + "/") + name), t);
			}
		}

		void editSI(final POIFSFileSystem poiFs, final POIFSDocumentPath path, final String name, final PropertySet si) throws IOException, WritingNotSupportedException {
			final DirectoryEntry de = getPath(poiFs, path);
			final PropertySet mps = new PropertySet(si);
			final Section s = mps.getSections().get(0);
			s.setProperty(PropertyIDMap.PID_AUTHOR, Variant.VT_LPSTR, "Rainer Klute");
			s.setProperty(PropertyIDMap.PID_TITLE, Variant.VT_LPWSTR, "Test");
			final InputStream pss = mps.toInputStream();
			de.createDocument(name, pss);
		}

		public void copy(final POIFSFileSystem poiFs, final POIFSDocumentPath path, final String name, final PropertySet ps) throws IOException, WritingNotSupportedException {
			final DirectoryEntry de = getPath(poiFs, path);
			final PropertySet mps = new PropertySet(ps);
			de.createDocument(name, mps.toInputStream());
		}

		public void copy(final POIFSFileSystem poiFs, final POIFSDocumentPath path, final String name, final DocumentInputStream stream) throws IOException {
			final DirectoryEntry de = getPath(poiFs, path);
			final ByteArrayOutputStream out = new ByteArrayOutputStream();
			int c;
			while ((c = stream.read()) != (-1))
				out.write(c);

			stream.close();
			out.close();
			final InputStream in = new ByteArrayInputStream(out.toByteArray());
			de.createDocument(name, in);
		}

		public void close() throws IOException {
			out = new FileOutputStream(dstName);
			poiFs.writeFilesystem(out);
			out.close();
		}

		private final Map<String, DirectoryEntry> paths = new HashMap<>();

		public DirectoryEntry getPath(final POIFSFileSystem poiFs, final POIFSDocumentPath path) {
			try {
				final String s = path.toString();
				DirectoryEntry de = paths.get(s);
				if (de != null)
					return de;

				int l = path.length();
				if (l == 0)
					de = poiFs.getRoot();
				else {
					de = getPath(poiFs, path.getParent());
					de = de.createDirectory(path.getComponent(((path.length()) - 1)));
				}
				paths.put(s, de);
				return de;
			} catch (IOException ex) {
				ex.printStackTrace(System.err);
				throw new RuntimeException(ex);
			}
		}
	}
}

