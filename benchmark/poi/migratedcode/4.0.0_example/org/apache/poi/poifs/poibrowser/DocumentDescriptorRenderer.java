package org.apache.poi.poifs.poibrowser;


import java.awt.Component;
import java.awt.Container;
import java.awt.Font;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import org.apache.poi.util.HexDump;


public class DocumentDescriptorRenderer extends DefaultTreeCellRenderer {
	@Override
	public Component getTreeCellRendererComponent(final JTree tree, final Object value, final boolean selectedCell, final boolean expanded, final boolean leaf, final int row, final boolean hasCellFocus) {
		final DocumentDescriptor d = ((DocumentDescriptor) (((DefaultMutableTreeNode) (value)).getUserObject()));
		final JPanel p = new JPanel();
		final JTextArea text = new JTextArea();
		text.append(renderAsString(d));
		text.setFont(new Font("Monospaced", Font.PLAIN, 10));
		p.add(text);
		if (selectedCell) {
			Util.invert(text);
		}
		return p;
	}

	protected String renderAsString(final DocumentDescriptor d) {
		return (((((((("Name: " + (d.name)) + " ") + (HexDump.toHex(d.name))) + "\n") + "Size: ") + (d.size)) + " bytes\n") + "First bytes: ") + (HexDump.toHex(d.bytes));
	}
}

