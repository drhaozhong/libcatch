package org.apache.poi.poifs.poibrowser;


import java.awt.Component;
import java.awt.Container;
import java.awt.Font;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;


public class DocumentDescriptorRenderer extends DefaultTreeCellRenderer {
	public Component getTreeCellRendererComponent(final JTree tree, final Object value, final boolean selected, final boolean expanded, final boolean leaf, final int row, final boolean hasFocus) {
		final DocumentDescriptor d = ((DocumentDescriptor) (((DefaultMutableTreeNode) (value)).getUserObject()));
		final JPanel p = new JPanel();
		final JTextArea text = new JTextArea();
		text.append(renderAsString(d));
		text.setFont(new Font("Monospaced", Font.PLAIN, 10));
		p.add(text);
		if (selected)
			Util.invert(text);

		return p;
	}

	protected String renderAsString(final DocumentDescriptor d) {
		final StringBuffer b = new StringBuffer();
		b.append("Name: ");
		b.append(d.name);
		b.append(" (");
		b.append(Codec.hexEncode(d.name));
		b.append(")  \n");
		b.append("Size: ");
		b.append(d.size);
		b.append(" bytes\n");
		b.append("First bytes: ");
		b.append(Codec.hexEncode(d.bytes));
		return b.toString();
	}
}

