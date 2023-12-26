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
import java.util.Iterator;
import java.util.Map;
import org.apache.poi.hpsf.HPSFRuntimeException;
import org.apache.poi.hpsf.MarkUnsupportedException;
import org.apache.poi.hpsf.MutablePropertySet;
import org.apache.poi.hpsf.NoPropertySetStreamException;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.Util;
import org.apache.poi.hpsf.WritingNotSupportedException;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
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
		r.read(new FileInputStream(originalFileName));
		cf.close();
		final POIFSFileSystem opfs = new POIFSFileSystem(new FileInputStream(originalFileName));
		final POIFSFileSystem cpfs = new POIFSFileSystem(new FileInputStream(copyFileName));
		final DirectoryEntry oRoot = opfs.getRoot();
		final DirectoryEntry cRoot = cpfs.getRoot();
		final StringBuffer messages = new StringBuffer();
		if (CopyCompare.equal(oRoot, cRoot, messages))
			System.out.println("Equal");
		else
			System.out.println(("Not equal: " + (messages.toString())));

	}

	private static boolean equal(final DirectoryEntry d1, final DirectoryEntry d2, final StringBuffer msg) throws IOException, UnsupportedEncodingException, MarkUnsupportedException, NoPropertySetStreamException {
		boolean equal = true;
		for (final Iterator i = d1.getEntries(); equal && (i.hasNext());) {
			final Entry e1 = ((Entry) (i.next()));
			final String n1 = e1.getName();
			Entry e2 = null;
			try {
				e2 = d2.getEntry(n1);
			} catch (FileNotFoundException ex) {
				msg.append((((("Document \"" + e1) + "\" exists, document \"") + e2) + "\" does not.\n"));
				equal = false;
				break;
			}
			if ((e1.isDirectoryEntry()) && (e2.isDirectoryEntry()))
				equal = CopyCompare.equal(((DirectoryEntry) (e1)), ((DirectoryEntry) (e2)), msg);
			else
				if ((e1.isDocumentEntry()) && (e2.isDocumentEntry()))
					equal = CopyCompare.equal(((DocumentEntry) (e1)), ((DocumentEntry) (e2)), msg);
				else {
					msg.append(((((("One of \"" + e1) + "\" and \"") + e2) + "\" is a ") + "document while the other one is a directory.\n"));
					equal = false;
				}

		}
		for (final Iterator i = d2.getEntries(); equal && (i.hasNext());) {
			final Entry e2 = ((Entry) (i.next()));
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
		boolean equal = true;
		final DocumentInputStream dis1 = new DocumentInputStream(d1);
		final DocumentInputStream dis2 = new DocumentInputStream(d2);
		if ((PropertySet.isPropertySetStream(dis1)) && (PropertySet.isPropertySetStream(dis2))) {
			final PropertySet ps1 = PropertySetFactory.create(dis1);
			final PropertySet ps2 = PropertySetFactory.create(dis2);
			equal = ps1.equals(ps2);
			if (!equal) {
				msg.append("Property sets are not equal.\n");
				return equal;
			}
		}else {
			int i1;
			int i2;
			do {
				i1 = dis1.read();
				i2 = dis2.read();
				if (i1 != i2) {
					equal = false;
					msg.append("Documents are not equal.\n");
					break;
				}
			} while (equal && (i1 == (-1)) );
		}
		return true;
	}

	static class CopyFile implements POIFSReaderListener {
		String dstName;

		OutputStream out;

		POIFSFileSystem poiFs;

		public CopyFile(final String dstName) {
			this.dstName = dstName;
			poiFs = new POIFSFileSystem();
		}

		public void processPOIFSReaderEvent(final POIFSReaderEvent event) {
			final POIFSDocumentPath path = event.getPath();
			final String name = event.getName();
			final DocumentInputStream stream = event.getStream();
			Throwable t = null;
			try {
				if (PropertySet.isPropertySetStream(stream)) {
					PropertySet ps = null;
					try {
						ps = PropertySetFactory.create(stream);
					} catch (NoPropertySetStreamException ex) {
					}
					copy(poiFs, path, name, ps);
				}else
					copy(poiFs, event.getPath(), event.getName(), stream);

			} catch (MarkUnsupportedException ex) {
				t = ex;
			} catch (IOException ex) {
				t = ex;
			} catch (WritingNotSupportedException ex) {
				t = ex;
			}
			if (t != null) {
				throw new HPSFRuntimeException(((((("Could not read file \"" + path) + "/") + name) + "\". Reason: ") + (Util.toString(t))));
			}
		}

		public void copy(final POIFSFileSystem poiFs, final POIFSDocumentPath path, final String name, final PropertySet ps) throws IOException, WritingNotSupportedException {
			final DirectoryEntry de = getPath(poiFs, path);
			final MutablePropertySet mps = new MutablePropertySet(ps);
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

		public void close() throws FileNotFoundException, IOException {
			out = new FileOutputStream(dstName);
			poiFs.writeFilesystem(out);
			out.close();
		}

		private final Map paths = new HashMap();

		public DirectoryEntry getPath(final POIFSFileSystem poiFs, final POIFSDocumentPath path) {
			try {
				final String s = path.toString();
				DirectoryEntry de = ((DirectoryEntry) (paths.get(s)));
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
				throw new RuntimeException(ex.toString());
			}
		}
	}
}

