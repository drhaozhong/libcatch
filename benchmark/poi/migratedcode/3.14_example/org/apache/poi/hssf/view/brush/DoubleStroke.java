package org.apache.poi.hssf.view.brush;


import java.awt.BasicStroke;
import java.awt.Shape;


public class DoubleStroke implements Brush {
	BasicStroke stroke1;

	BasicStroke stroke2;

	public DoubleStroke(float width1, float width2) {
		stroke1 = new BasicStroke(width1);
		stroke2 = new BasicStroke(width2);
	}

	public Shape createStrokedShape(Shape s) {
		Shape outline = stroke1.createStrokedShape(s);
		return stroke2.createStrokedShape(outline);
	}

	public float getLineWidth() {
		return (stroke1.getLineWidth()) + (2 * (stroke2.getLineWidth()));
	}
}

