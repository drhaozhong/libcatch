package org.apache.poi.hssf.view;


import java.awt.Color;
import java.awt.Font;
import java.util.Map;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.util.HSSFColor;

import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.BLACK;
import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.WHITE;


public class SVTableUtils {
	private static final Map<Integer, HSSFColor> colors = HSSFColor.getIndexHash();

	public static final Color black = SVTableUtils.getAWTColor(BLACK);

	public static final Color white = SVTableUtils.getAWTColor(WHITE);

	public static final Border noFocusBorder = new EmptyBorder(1, 1, 1, 1);

	public static Font makeFont(HSSFFont font) {
		boolean isbold = font.getBold();
		boolean isitalics = font.getItalic();
		int fontstyle = Font.PLAIN;
		if (isbold) {
			fontstyle = Font.BOLD;
		}
		if (isitalics) {
			fontstyle = fontstyle | (Font.ITALIC);
		}
		int fontheight = font.getFontHeightInPoints();
		if (fontheight == 9) {
			fontheight = 10;
		}
		return new Font(font.getFontName(), fontstyle, fontheight);
	}

	static Color getAWTColor(int index, Color deflt) {
		HSSFColor clr = SVTableUtils.colors.get(index);
		if (clr == null) {
			return deflt;
		}
		short[] rgb = clr.getTriplet();
		return new Color(rgb[0], rgb[1], rgb[2]);
	}

	static Color getAWTColor(HSSFColor.HSSFColorPredefined clr) {
		short[] rgb = clr.getTriplet();
		return new Color(rgb[0], rgb[1], rgb[2]);
	}
}

