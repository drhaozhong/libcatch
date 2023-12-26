package org.apache.poi.ss.examples.html;


import java.util.Formatter;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;


public class XSSFHtmlHelper implements HtmlHelper {
	public void colorStyles(CellStyle style, Formatter out) {
		XSSFCellStyle cs = ((XSSFCellStyle) (style));
		styleColor(out, "background-color", cs.getFillForegroundXSSFColor());
		styleColor(out, "text-color", cs.getFont().getXSSFColor());
	}

	private void styleColor(Formatter out, String attr, XSSFColor color) {
		if ((color == null) || (color.isAuto())) {
			return;
		}
		byte[] rgb = color.getRGB();
		if (rgb == null) {
			return;
		}
		out.format("  %s: #%02x%02x%02x;%n", attr, rgb[0], rgb[1], rgb[2]);
		byte[] argb = color.getARGB();
		if (argb == null) {
			return;
		}
		out.format("  %s: rgba(0x%02x, 0x%02x, 0x%02x, 0x%02x);%n", attr, argb[3], argb[0], argb[1], argb[2]);
	}
}

