package org.apache.poi.hpsf.examples;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.hpsf.HPSFRuntimeException;
import org.apache.poi.hpsf.MarkUnsupportedException;
import org.apache.poi.hpsf.NoPropertySetStreamException;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.WritingNotSupportedException;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.util.IOUtils;
import org.apache.poi.util.TempFile;


public class CopyCompare {
	public static void main(final String[] args) throws IOException, UnsupportedEncodingException, MarkUnsupportedException, NoPropertySetStreamException {
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
		FileInputStream fis = new FileInputStream(originalFileName);
		r.read(fis);
		fis.close();
		cf.close();
		POIFSFileSystem opfs = null;
		POIFSFileSystem cpfs = null;
		try {
			opfs = new POIFSFileSystem(new File(originalFileName));
			cpfs = new POIFSFileSystem(new File(copyFileName));
			final DirectoryEntry oRoot = opfs.getRoot();
			final DirectoryEntry cRoot = cpfs.getRoot();
			final StringBuffer messages = new StringBuffer();
			if (CopyCompare.equal(oRoot, cRoot, messages)) {
				System.out.println("Equal");
			}else {
				System.out.println(("Not equal: " + messages));
			}
		} finally {
			IOUtils.closeQuietly(cpfs);
			IOUtils.closeQuietly(opfs);
		}
	}

	private static boolean equal(final DirectoryEntry d1, final DirectoryEntry d2, final StringBuffer msg) throws IOException, UnsupportedEncodingException, MarkUnsupportedException, NoPropertySetStreamException {
		boolean equal = true;
		for (final Entry e1 : d1) {
			final String n1 = e1.getName();
			if (!(d2.hasEntry(n1))) {
				msg.append((("Document \"" + n1) + "\" exists only in the source.\n"));
				equal = false;
				break;
			}
			Entry e2 = d2.getEntry(n1);
			if ((e1.isDirectoryEntry()) && (e2.isDirectoryEntry())) {
				equal = CopyCompare.equal(((DirectoryEntry) (e1)), ((DirectoryEntry) (e2)), msg);
			}else
				if ((e1.isDocumentEntry()) && (e2.isDocumentEntry())) {
					equal = CopyCompare.equal(((DocumentEntry) (e1)), ((DocumentEntry) (e2)), msg);
				}else {
					msg.append(((((("One of \"" + e1) + "\" and \"") + e2) + "\" is a ") + "document while the other one is a directory.\n"));
					equal = false;
				}

		}
		for (final Entry e2 : d2) {
			final String n2 = e2.getName();
			Entry e1 = null;
			try {
				e1 = d1.getEntry(n2);
			} catch (FileNotFoundException ex) {
				msg.append((((("Document \"" + e2) + "\" exitsts, document \"") + e1) + "\" does not.\n"));
				equal = false;
				break;
			}
		}
		return equal;
	}

	private static boolean equal(final DocumentEntry d1, final DocumentEntry d2, final StringBuffer msg) throws IOException, UnsupportedEncodingException, MarkUnsupportedException, NoPropertySetStreamException {
		final DocumentInputStream dis1 = new DocumentInputStream(d1);
		final DocumentInputStream dis2 = new DocumentInputStream(d2);
		try {
			if ((PropertySet.isPropertySetStream(dis1)) && (PropertySet.isPropertySetStream(dis2))) {
				final PropertySet ps1 = PropertySetFactory.create(dis1);
				final PropertySet ps2 = PropertySetFactory.create(dis2);
				if (!(ps1.equals(ps2))) {
					msg.append("Property sets are not equal.\n");
					return false;
				}
			}else {
				int i1;
				int i2;
				do {
					i1 = dis1.read();
					i2 = dis2.read();
					if (i1 != i2) {
						msg.append("Documents are not equal.\n");
						return false;
					}
				} while (i1 > (-1) );
			}
		} finally {
			dis2.close();
			dis1.close();
		}
		return true;
	}

	static class CopyFile implements POIFSReaderListener {
		private String dstName;

		private OutputStream out;

		private POIFSFileSystem poiFs;

		public CopyFile(final String dstName) {
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
			} catch (MarkUnsupportedException ex) {
				t = ex;
			} catch (IOException ex) {
				t = ex;
			} catch (WritingNotSupportedException ex) {
				t = ex;
			}
			if (t != null) {
				throw new HPSFRuntimeException(((("Could not read file \"" + path) + "/") + name), t);
			}
		}

		public void copy(final POIFSFileSystem poiFs, final POIFSDocumentPath path, final String name, final PropertySet ps) throws IOException, WritingNotSupportedException {
			final DirectoryEntry de = getPath(poiFs, path);
			final PropertySet mps = new PropertySet(ps);
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

		public void close() throws FileNotFoundException, IOException {
			out = new FileOutputStream(dstName);
			poiFs.writeFilesystem(out);
			out.close();
		}

		private final Map<String, DirectoryEntry> paths = new HashMap<String, DirectoryEntry>();

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
				ex.printStackTrace(System.err);
				throw new RuntimeException(ex.toString());
			}
		}
	}
}

