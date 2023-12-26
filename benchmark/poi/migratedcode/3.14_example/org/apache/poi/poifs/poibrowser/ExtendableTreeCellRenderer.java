package org.apache.poi.poifs.poibrowser;


import java.awt.Component;
import java.util.HashMap;
import java.util.Map;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreeCellRenderer;


public class ExtendableTreeCellRenderer implements TreeCellRenderer {
	protected Map<Class<?>, TreeCellRenderer> renderers;

	public ExtendableTreeCellRenderer() {
		renderers = new HashMap<Class<?>, TreeCellRenderer>();
		register(Object.class, new DefaultTreeCellRenderer() {
			public Component getTreeCellRendererComponent(JTree tree, Object value, boolean selectedCell, boolean expanded, boolean leaf, int row, boolean hasCellFocus) {
				final String s = value.toString();
				final JLabel l = new JLabel((s + "  "));
				if (selected) {
					Util.invert(l);
					l.setOpaque(true);
				}
				return l;
			}
		});
	}

	public void register(final Class<?> c, final TreeCellRenderer renderer) {
		renderers.put(c, renderer);
	}

	public void unregister(final Class<?> c) {
		if (c == (Object.class))
			throw new IllegalArgumentException("Renderer for Object cannot be unregistered.");

		renderers.put(c, null);
	}

	public Component getTreeCellRendererComponent(final JTree tree, final Object value, final boolean selected, final boolean expanded, final boolean leaf, final int row, final boolean hasFocus) {
		final String NULL = "null";
		TreeCellRenderer r;
		Object userObject;
		if (value == null)
			userObject = NULL;
		else {
			userObject = ((DefaultMutableTreeNode) (value)).getUserObject();
			if (userObject == null)
				userObject = NULL;

		}
		r = findRenderer(userObject.getClass());
		return r.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, hasFocus);
	}

	protected TreeCellRenderer findRenderer(final Class<?> c) {
		final TreeCellRenderer r = renderers.get(c);
		if (r != null)
			return r;

		final Class<?> superclass = c.getSuperclass();
		if (superclass != null) {
			return findRenderer(superclass);
		}
		return null;
	}
}

