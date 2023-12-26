package org.apache.poi.hpsf.examples;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.hpsf.DocumentSummaryInformation;
import org.apache.poi.hpsf.HPSFRuntimeException;
import org.apache.poi.hpsf.MarkUnsupportedException;
import org.apache.poi.hpsf.NoPropertySetStreamException;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.hpsf.UnexpectedPropertySetTypeException;
import org.apache.poi.hpsf.WritingNotSupportedException;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.EntryUtils;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.util.TempFile;


public final class CopyCompare {
	private CopyCompare() {
	}

	public static void main(final String[] args) throws IOException, UnsupportedEncodingException {
		String originalFileName = null;
		String copyFileName = null;
		if ((args.length) == 1) {
			originalFileName = args[0];
			File f = TempFile.createTempFile("CopyOfPOIFileSystem-", ".ole2");
			f.deleteOnExit();
			copyFileName = f.getAbsolutePath();
		}else
			if ((args.length) == 2) {
				originalFileName = args[0];
				copyFileName = args[1];
			}else {
				System.err.println((("Usage: " + (CopyCompare.class.getName())) + "originPOIFS [copyPOIFS]"));
				System.exit(1);
			}

		final POIFSReader r = new POIFSReader();
		final CopyCompare.CopyFile cf = new CopyCompare.CopyFile(copyFileName);
		r.registerListener(cf);
		r.setNotifyEmptyDirectories(true);
		r.read(new File(originalFileName));
		cf.close();
		try (POIFSFileSystem opfs = new POIFSFileSystem(new File(originalFileName));POIFSFileSystem cpfs = new POIFSFileSystem(new File(copyFileName))) {
			final DirectoryEntry oRoot = opfs.getRoot();
			final DirectoryEntry cRoot = cpfs.getRoot();
			System.out.println((EntryUtils.areDirectoriesIdentical(oRoot, cRoot) ? "Equal" : "Not equal"));
		}
	}

	static class CopyFile implements POIFSReaderListener {
		private String dstName;

		private OutputStream out;

		private POIFSFileSystem poiFs;

		CopyFile(final String dstName) {
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
				if ((stream != null) && (PropertySet.isPropertySetStream(stream))) {
					PropertySet ps = null;
					try {
						ps = PropertySetFactory.create(stream);
					} catch (NoPropertySetStreamException ex) {
					}
					copy(poiFs, path, name, ps);
				}else {
					copy(poiFs, path, name, stream);
				}
			} catch (MarkUnsupportedException | WritingNotSupportedException | IOException ex) {
				t = ex;
			}
			if (t != null) {
				throw new HPSFRuntimeException(((("Could not read file \"" + path) + "/") + name), t);
			}
		}

		public void copy(final POIFSFileSystem poiFs, final POIFSDocumentPath path, final String name, final PropertySet ps) throws IOException, WritingNotSupportedException {
			final DirectoryEntry de = getPath(poiFs, path);
			final PropertySet mps;
			try {
				if (ps instanceof DocumentSummaryInformation) {
					mps = new DocumentSummaryInformation(ps);
				}else
					if (ps instanceof SummaryInformation) {
						mps = new SummaryInformation(ps);
					}else {
						mps = new PropertySet(ps);
					}

			} catch (UnexpectedPropertySetTypeException e) {
				throw new IOException(e);
			}
			de.createDocument(name, mps.toInputStream());
		}

		public void copy(final POIFSFileSystem poiFs, final POIFSDocumentPath path, final String name, final DocumentInputStream stream) throws IOException {
			final DirectoryEntry de = getPath(poiFs, path);
			if ((stream == null) || (name == null)) {
				return;
			}
			final ByteArrayOutputStream out = new ByteArrayOutputStream();
			int c;
			while ((c = stream.read()) != (-1)) {
				out.write(c);
			} 
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
				if (l == 0) {
					de = poiFs.getRoot();
				}else {
					de = getPath(poiFs, path.getParent());
					de = de.createDirectory(path.getComponent(((path.length()) - 1)));
				}
				paths.put(s, de);
				return de;
			} catch (IOException ex) {
				throw new RuntimeException(ex);
			}
		}
	}
}

