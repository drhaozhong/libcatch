package org.apache.poi.hssf.view.brush;


import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.geom.AffineTransform;
import java.util.ArrayList;
import java.util.List;
import javax.swing.JComponent;
import org.apache.poi.hssf.view.brush.PendingPaintings.Painting;


public class PendingPaintings {
	public static final String PENDING_PAINTINGS = PendingPaintings.class.getSimpleName();

	private final List<PendingPaintings.Painting> paintings;

	public static class Painting {
		final Stroke stroke;

		final Color color;

		final Shape shape;

		final AffineTransform transform;

		public Painting(Stroke stroke, Color color, Shape shape, AffineTransform transform) {
			this.color = color;
			this.shape = shape;
			this.stroke = stroke;
			this.transform = transform;
		}

		public void draw(Graphics2D g) {
			g.setTransform(transform);
			g.setStroke(stroke);
			g.setColor(color);
			g.draw(shape);
		}
	}

	public PendingPaintings(JComponent parent) {
		paintings = new ArrayList<>();
		parent.putClientProperty(PendingPaintings.PENDING_PAINTINGS, this);
	}

	public void clear() {
		paintings.clear();
	}

	public void paint(Graphics2D g) {
		g.setBackground(Color.CYAN);
		AffineTransform origTransform = g.getTransform();
		for (PendingPaintings.Painting c : paintings) {
			c.draw(g);
		}
		g.setTransform(origTransform);
		clear();
	}

	public static void add(JComponent c, Graphics2D g, Stroke stroke, Color color, Shape shape) {
		PendingPaintings.add(c, new PendingPaintings.Painting(stroke, color, shape, g.getTransform()));
	}

	public static void add(JComponent c, PendingPaintings.Painting newPainting) {
		PendingPaintings pending = PendingPaintings.pendingPaintingsFor(c);
		if (pending != null) {
			pending.paintings.add(newPainting);
		}
	}

	public static PendingPaintings pendingPaintingsFor(JComponent c) {
		for (Component parent = c; parent != null; parent = parent.getParent()) {
			if (parent instanceof JComponent) {
				JComponent jc = ((JComponent) (parent));
				Object pd = jc.getClientProperty(PendingPaintings.PENDING_PAINTINGS);
				if (pd != null)
					return ((PendingPaintings) (pd));

			}
		}
		return null;
	}
}

