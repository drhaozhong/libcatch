package org.apache.poi.hssf.view.brush;


import java.awt.BasicStroke;


public class BasicBrush extends BasicStroke implements Brush {
	public BasicBrush(float width) {
		super(width);
	}

	public BasicBrush(float width, int cap, int join) {
		super(width, cap, join);
	}

	public BasicBrush(float width, int cap, int join, float[] dashes, int dashPos) {
		super(width, cap, join, 11.0F, dashes, dashPos);
	}
}

