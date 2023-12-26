package org.apache.poi.poifs.poibrowser;


import java.awt.Component;
import java.awt.Container;
import java.awt.Frame;
import java.awt.Window;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.MutableTreeNode;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;


@SuppressWarnings("serial")
public class POIBrowser extends JFrame {
	protected MutableTreeNode rootNode;

	public static void main(String[] args) {
		new POIBrowser().run(args);
	}

	protected void run(String[] args) {
		addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});
		rootNode = new DefaultMutableTreeNode("POI Filesystems");
		DefaultTreeModel treeModel = new DefaultTreeModel(rootNode);
		final JTree treeUI = new JTree(treeModel);
		getContentPane().add(new JScrollPane(treeUI));
		int displayedFiles = 0;
		for (int i = 0; i < (args.length); i++) {
			final String filename = args[i];
			try {
				POIFSReader r = new POIFSReader();
				r.registerListener(new TreeReaderListener(filename, rootNode));
				r.read(new FileInputStream(filename));
				displayedFiles++;
			} catch (IOException ex) {
				System.err.println(((filename + ": ") + ex));
			} catch (Exception t) {
				System.err.println((("Unexpected exception while reading \"" + filename) + "\":"));
				t.printStackTrace(System.err);
			}
		}
		if (displayedFiles == 0) {
			System.out.println("No POI filesystem(s) to display.");
			System.exit(0);
		}
		treeUI.setRootVisible(true);
		treeUI.setShowsRootHandles(true);
		ExtendableTreeCellRenderer etcr = new ExtendableTreeCellRenderer();
		etcr.register(DocumentDescriptor.class, new DocumentDescriptorRenderer());
		etcr.register(PropertySetDescriptor.class, new PropertySetDescriptorRenderer());
		treeUI.setCellRenderer(etcr);
		setSize(600, 450);
		setTitle("POI Browser 0.09");
		setVisible(true);
	}
}

