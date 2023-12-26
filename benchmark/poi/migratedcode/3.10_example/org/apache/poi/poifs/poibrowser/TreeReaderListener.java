package org.apache.poi.poifs.poibrowser;


import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.MutableTreeNode;
import org.apache.poi.hpsf.HPSFException;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;


public class TreeReaderListener implements POIFSReaderListener {
	protected MutableTreeNode rootNode;

	protected Map pathToNode;

	protected String filename;

	public TreeReaderListener(final String filename, final MutableTreeNode rootNode) {
		this.filename = filename;
		this.rootNode = rootNode;
		pathToNode = new HashMap(15);
	}

	private int nrOfBytes = 50;

	public void setNrOfBytes(final int nrOfBytes) {
		this.nrOfBytes = nrOfBytes;
	}

	public int getNrOfBytes() {
		return nrOfBytes;
	}

	public void processPOIFSReaderEvent(final POIFSReaderEvent event) {
		DocumentDescriptor d;
		final DocumentInputStream is = event.getStream();
		if (!(is.markSupported()))
			throw new UnsupportedOperationException(((is.getClass().getName()) + " does not support mark()."));

		try {
			d = new PropertySetDescriptor(event.getName(), event.getPath(), is, nrOfBytes);
		} catch (HPSFException ex) {
			d = new DocumentDescriptor(event.getName(), event.getPath(), is, nrOfBytes);
		} catch (Throwable t) {
			System.err.println(((("Unexpected exception while processing " + (event.getName())) + " in ") + (event.getPath().toString())));
			t.printStackTrace(System.err);
			throw new RuntimeException(t.getMessage());
		}
		is.close();
		final MutableTreeNode parentNode = getNode(d.path, filename, rootNode);
		final MutableTreeNode nameNode = new DefaultMutableTreeNode(d.name);
		parentNode.insert(nameNode, 0);
		final MutableTreeNode dNode = new DefaultMutableTreeNode(d);
		nameNode.insert(dNode, 0);
	}

	private MutableTreeNode getNode(final POIFSDocumentPath path, final String fsName, final MutableTreeNode root) {
		MutableTreeNode n = ((MutableTreeNode) (pathToNode.get(path)));
		if (n != null)
			return n;

		if ((path.length()) == 0) {
			n = ((MutableTreeNode) (pathToNode.get(fsName)));
			if (n == null) {
				n = new DefaultMutableTreeNode(fsName);
				pathToNode.put(fsName, n);
				root.insert(n, 0);
			}
			return n;
		}
		final String name = path.getComponent(((path.length()) - 1));
		final POIFSDocumentPath parentPath = path.getParent();
		final MutableTreeNode parentNode = getNode(parentPath, fsName, root);
		n = new DefaultMutableTreeNode(name);
		pathToNode.put(path, n);
		parentNode.insert(n, 0);
		return n;
	}
}

