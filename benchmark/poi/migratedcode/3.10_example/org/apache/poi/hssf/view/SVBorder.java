package org.apache.poi.hssf.view;


import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics;
import javax.swing.border.AbstractBorder;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;


public class SVBorder extends AbstractBorder {
	private Color northColor = null;

	private Color eastColor = null;

	private Color southColor = null;

	private Color westColor = null;

	private int northBorderType = HSSFCellStyle.BORDER_NONE;

	private int eastBorderType = HSSFCellStyle.BORDER_NONE;

	private int southBorderType = HSSFCellStyle.BORDER_NONE;

	private int westBorderType = HSSFCellStyle.BORDER_NONE;

	private boolean northBorder = false;

	private boolean eastBorder = false;

	private boolean southBorder = false;

	private boolean westBorder = false;

	private boolean selected = false;

	public void setBorder(Color northColor, Color eastColor, Color southColor, Color westColor, int northBorderType, int eastBorderType, int southBorderType, int westBorderType, boolean selected) {
		this.eastColor = eastColor;
		this.southColor = southColor;
		this.westColor = westColor;
		this.northBorderType = northBorderType;
		this.eastBorderType = eastBorderType;
		this.southBorderType = southBorderType;
		this.westBorderType = westBorderType;
		this.northBorder = northBorderType != (HSSFCellStyle.BORDER_NONE);
		this.eastBorder = eastBorderType != (HSSFCellStyle.BORDER_NONE);
		this.southBorder = southBorderType != (HSSFCellStyle.BORDER_NONE);
		this.westBorder = westBorderType != (HSSFCellStyle.BORDER_NONE);
		this.selected = selected;
	}

	public void paintBorder(Component c, Graphics g, int x, int y, int width, int height) {
		Color oldColor = g.getColor();
		paintSelectedBorder(g, x, y, width, height);
		paintNormalBorders(g, x, y, width, height);
		paintDottedBorders(g, x, y, width, height);
		paintDashedBorders(g, x, y, width, height);
		paintDoubleBorders(g, x, y, width, height);
		paintDashDotDotBorders(g, x, y, width, height);
		g.setColor(oldColor);
	}

	private void paintSelectedBorder(Graphics g, int x, int y, int width, int height) {
		if (selected) {
			g.setColor(Color.black);
			g.drawRect(x, y, (width - 1), (height - 1));
			g.fillRect(((x + width) - 5), ((y + height) - 5), 5, 5);
		}
	}

	private void paintNormalBorders(Graphics g, int x, int y, int width, int height) {
		if ((northBorder) && ((((northBorderType) == (HSSFCellStyle.BORDER_THIN)) || ((northBorderType) == (HSSFCellStyle.BORDER_MEDIUM))) || ((northBorderType) == (HSSFCellStyle.BORDER_THICK)))) {
			int thickness = getThickness(northBorderType);
			g.setColor(northColor);
			for (int k = 0; k < thickness; k++) {
				g.drawLine(x, (y + k), width, (y + k));
			}
		}
		if ((eastBorder) && ((((eastBorderType) == (HSSFCellStyle.BORDER_THIN)) || ((eastBorderType) == (HSSFCellStyle.BORDER_MEDIUM))) || ((eastBorderType) == (HSSFCellStyle.BORDER_THICK)))) {
			int thickness = getThickness(eastBorderType);
			g.setColor(eastColor);
			for (int k = 0; k < thickness; k++) {
				g.drawLine((width - k), y, (width - k), height);
			}
		}
		if ((southBorder) && ((((southBorderType) == (HSSFCellStyle.BORDER_THIN)) || ((southBorderType) == (HSSFCellStyle.BORDER_MEDIUM))) || ((southBorderType) == (HSSFCellStyle.BORDER_THICK)))) {
			int thickness = getThickness(southBorderType);
			g.setColor(southColor);
			for (int k = 0; k < thickness; k++) {
				g.drawLine(x, (height - k), width, (height - k));
			}
		}
		if ((westBorder) && ((((westBorderType) == (HSSFCellStyle.BORDER_THIN)) || ((westBorderType) == (HSSFCellStyle.BORDER_MEDIUM))) || ((westBorderType) == (HSSFCellStyle.BORDER_THICK)))) {
			int thickness = getThickness(westBorderType);
			g.setColor(westColor);
			for (int k = 0; k < thickness; k++) {
				g.drawLine((x + k), y, (x + k), height);
			}
		}
	}

	private void paintDottedBorders(Graphics g, int x, int y, int width, int height) {
		if ((northBorder) && ((northBorderType) == (HSSFCellStyle.BORDER_DOTTED))) {
			int thickness = getThickness(northBorderType);
			g.setColor(northColor);
			for (int k = 0; k < thickness; k++) {
				for (int xc = x; xc < width; xc = xc + 2) {
					g.drawLine(xc, (y + k), xc, (y + k));
				}
			}
		}
		if ((eastBorder) && ((eastBorderType) == (HSSFCellStyle.BORDER_DOTTED))) {
			int thickness = getThickness(eastBorderType);
			thickness++;
			g.setColor(eastColor);
			for (int k = 0; k < thickness; k++) {
				for (int yc = y; yc < height; yc = yc + 2) {
					g.drawLine((width - k), yc, (width - k), yc);
				}
			}
		}
		if ((southBorder) && ((southBorderType) == (HSSFCellStyle.BORDER_DOTTED))) {
			int thickness = getThickness(southBorderType);
			thickness++;
			g.setColor(southColor);
			for (int k = 0; k < thickness; k++) {
				for (int xc = x; xc < width; xc = xc + 2) {
					g.drawLine(xc, (height - k), xc, (height - k));
				}
			}
		}
		if ((westBorder) && ((westBorderType) == (HSSFCellStyle.BORDER_DOTTED))) {
			int thickness = getThickness(westBorderType);
			g.setColor(westColor);
			for (int k = 0; k < thickness; k++) {
				for (int yc = y; yc < height; yc = yc + 2) {
					g.drawLine((x + k), yc, (x + k), yc);
				}
			}
		}
	}

	private void paintDashedBorders(Graphics g, int x, int y, int width, int height) {
		if ((northBorder) && (((northBorderType) == (HSSFCellStyle.BORDER_DASHED)) || ((northBorderType) == (HSSFCellStyle.BORDER_HAIR)))) {
			int thickness = getThickness(northBorderType);
			int dashlength = 1;
			if ((northBorderType) == (HSSFCellStyle.BORDER_DASHED))
				dashlength = 2;

			g.setColor(northColor);
			for (int k = 0; k < thickness; k++) {
				for (int xc = x; xc < width; xc = xc + 5) {
					g.drawLine(xc, (y + k), (xc + dashlength), (y + k));
				}
			}
		}
		if ((eastBorder) && (((eastBorderType) == (HSSFCellStyle.BORDER_DASHED)) || ((eastBorderType) == (HSSFCellStyle.BORDER_HAIR)))) {
			int thickness = getThickness(eastBorderType);
			thickness++;
			int dashlength = 1;
			if ((eastBorderType) == (HSSFCellStyle.BORDER_DASHED))
				dashlength = 2;

			g.setColor(eastColor);
			for (int k = 0; k < thickness; k++) {
				for (int yc = y; yc < height; yc = yc + 5) {
					g.drawLine((width - k), yc, (width - k), (yc + dashlength));
				}
			}
		}
		if ((southBorder) && (((southBorderType) == (HSSFCellStyle.BORDER_DASHED)) || ((southBorderType) == (HSSFCellStyle.BORDER_HAIR)))) {
			int thickness = getThickness(southBorderType);
			thickness++;
			int dashlength = 1;
			if ((southBorderType) == (HSSFCellStyle.BORDER_DASHED))
				dashlength = 2;

			g.setColor(southColor);
			for (int k = 0; k < thickness; k++) {
				for (int xc = x; xc < width; xc = xc + 5) {
					g.drawLine(xc, (height - k), (xc + dashlength), (height - k));
				}
			}
		}
		if ((westBorder) && (((westBorderType) == (HSSFCellStyle.BORDER_DASHED)) || ((westBorderType) == (HSSFCellStyle.BORDER_HAIR)))) {
			int thickness = getThickness(westBorderType);
			int dashlength = 1;
			if ((westBorderType) == (HSSFCellStyle.BORDER_DASHED))
				dashlength = 2;

			g.setColor(westColor);
			for (int k = 0; k < thickness; k++) {
				for (int yc = y; yc < height; yc = yc + 5) {
					g.drawLine((x + k), yc, (x + k), (yc + dashlength));
				}
			}
		}
	}

	private void paintDoubleBorders(Graphics g, int x, int y, int width, int height) {
		if ((northBorder) && ((northBorderType) == (HSSFCellStyle.BORDER_DOUBLE))) {
			g.setColor(northColor);
			int leftx = x;
			int rightx = width;
			if (westBorder)
				leftx = x + 3;

			if (eastBorder)
				rightx = width - 3;

			g.drawLine(x, y, width, y);
			g.drawLine(leftx, (y + 2), rightx, (y + 2));
		}
		if ((eastBorder) && ((eastBorderType) == (HSSFCellStyle.BORDER_DOUBLE))) {
			int thickness = getThickness(eastBorderType);
			thickness++;
			g.setColor(eastColor);
			int topy = y;
			int bottomy = height;
			if (northBorder)
				topy = y + 3;

			if (southBorder)
				bottomy = height - 3;

			g.drawLine((width - 1), y, (width - 1), height);
			g.drawLine((width - 3), topy, (width - 3), bottomy);
		}
		if ((southBorder) && ((southBorderType) == (HSSFCellStyle.BORDER_DOUBLE))) {
			g.setColor(southColor);
			int leftx = y;
			int rightx = width;
			if (westBorder)
				leftx = x + 3;

			if (eastBorder)
				rightx = width - 3;

			g.drawLine(x, (height - 1), width, (height - 1));
			g.drawLine(leftx, (height - 3), rightx, (height - 3));
		}
		if ((westBorder) && ((westBorderType) == (HSSFCellStyle.BORDER_DOUBLE))) {
			int thickness = getThickness(westBorderType);
			g.setColor(westColor);
			int topy = y;
			int bottomy = height - 3;
			if (northBorder)
				topy = y + 2;

			if (southBorder)
				bottomy = height - 3;

			g.drawLine(x, y, x, height);
			g.drawLine((x + 2), topy, (x + 2), bottomy);
		}
	}

	private void paintDashDotDotBorders(Graphics g, int x, int y, int width, int height) {
		if ((northBorder) && (((northBorderType) == (HSSFCellStyle.BORDER_DASH_DOT_DOT)) || ((northBorderType) == (HSSFCellStyle.BORDER_MEDIUM_DASH_DOT_DOT)))) {
			int thickness = getThickness(northBorderType);
			g.setColor(northColor);
			for (int l = x; l < width;) {
				l = l + (drawDashDotDot(g, l, y, thickness, true, true));
			}
		}
		if ((eastBorder) && (((eastBorderType) == (HSSFCellStyle.BORDER_DASH_DOT_DOT)) || ((eastBorderType) == (HSSFCellStyle.BORDER_MEDIUM_DASH_DOT_DOT)))) {
			int thickness = getThickness(eastBorderType);
			g.setColor(eastColor);
			for (int l = y; l < height;) {
				l = l + (drawDashDotDot(g, (width - 1), l, thickness, false, false));
			}
		}
		if ((southBorder) && (((southBorderType) == (HSSFCellStyle.BORDER_DASH_DOT_DOT)) || ((southBorderType) == (HSSFCellStyle.BORDER_MEDIUM_DASH_DOT_DOT)))) {
			int thickness = getThickness(southBorderType);
			g.setColor(southColor);
			for (int l = x; l < width;) {
				l = l + (drawDashDotDot(g, l, (height - 1), thickness, true, false));
			}
		}
		if ((westBorder) && (((westBorderType) == (HSSFCellStyle.BORDER_DASH_DOT_DOT)) || ((westBorderType) == (HSSFCellStyle.BORDER_MEDIUM_DASH_DOT_DOT)))) {
			int thickness = getThickness(westBorderType);
			g.setColor(westColor);
			for (int l = y; l < height;) {
				l = l + (drawDashDotDot(g, x, l, thickness, false, true));
			}
		}
	}

	private int drawDashDotDot(Graphics g, int x, int y, int thickness, boolean horizontal, boolean rightBottom) {
		for (int t = 0; t < thickness; t++) {
			if (!rightBottom) {
				t = 0 - t;
			}
			if (horizontal) {
				g.drawLine(x, (y + t), (x + 5), (y + t));
				g.drawLine((x + 8), (y + t), (x + 10), (y + t));
				g.drawLine((x + 13), (y + t), (x + 15), (y + t));
			}else {
				g.drawLine((x + t), y, (x + t), (y + 5));
				g.drawLine((x + t), (y + 8), (x + t), (y + 10));
				g.drawLine((x + t), (y + 13), (x + t), (y + 15));
			}
		}
		return 18;
	}

	private int getThickness(int thickness) {
		int retval = 1;
		switch (thickness) {
			case HSSFCellStyle.BORDER_THIN :
				retval = 2;
				break;
			case HSSFCellStyle.BORDER_MEDIUM :
				retval = 3;
				break;
			case HSSFCellStyle.BORDER_THICK :
				retval = 4;
				break;
			case HSSFCellStyle.BORDER_DASHED :
				retval = 1;
				break;
			case HSSFCellStyle.BORDER_DASH_DOT_DOT :
				retval = 1;
				break;
			case HSSFCellStyle.BORDER_MEDIUM_DASH_DOT_DOT :
				retval = 3;
				break;
			case HSSFCellStyle.BORDER_HAIR :
				retval = 1;
				break;
			default :
				retval = 1;
		}
		return retval;
	}
}

