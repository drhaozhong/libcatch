package org.apache.poi.hssf.usermodel.examples;


import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.RenderingHints;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.EscherGraphics;
import org.apache.poi.hssf.usermodel.EscherGraphics2d;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFClientAnchor;
import org.apache.poi.hssf.usermodel.HSSFPatriarch;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFShapeGroup;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;


public class OfficeDrawingWithGraphics {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet = wb.createSheet("my drawing");
		sheet.setColumnWidth(1, (256 * 27));
		HSSFRow row1 = sheet.createRow(0);
		row1.setHeightInPoints((10 * 15));
		HSSFRow row2 = sheet.createRow(1);
		row2.setHeightInPoints((5 * 15));
		HSSFRow row3 = sheet.createRow(2);
		row3.setHeightInPoints((10 * 15));
		row1.createCell(0).setCellValue("C");
		row2.createCell(0).setCellValue("A");
		row3.createCell(0).setCellValue("B");
		HSSFPatriarch patriarch = sheet.createDrawingPatriarch();
		HSSFClientAnchor a;
		HSSFShapeGroup group;
		EscherGraphics g;
		EscherGraphics2d g2d;
		a = new HSSFClientAnchor(0, 0, 1023, 255, ((short) (1)), 0, ((short) (1)), 0);
		group = patriarch.createGroup(a);
		group.setCoordinates(0, 0, 320, 276);
		float verticalPointsPerPixel = (a.getAnchorHeightInPoints(sheet)) / (Math.abs(((group.getY2()) - (group.getY1()))));
		g = new EscherGraphics(group, wb, Color.black, verticalPointsPerPixel);
		g2d = new EscherGraphics2d(g);
		OfficeDrawingWithGraphics.drawStar(g2d);
		a = new HSSFClientAnchor(0, 0, 1023, 255, ((short) (1)), 1, ((short) (1)), 1);
		group = patriarch.createGroup(a);
		group.setCoordinates(0, 0, 640, 276);
		verticalPointsPerPixel = (a.getAnchorHeightInPoints(sheet)) / (Math.abs(((group.getY2()) - (group.getY1()))));
		g = new EscherGraphics(group, wb, Color.black, verticalPointsPerPixel);
		g2d = new EscherGraphics2d(g);
		OfficeDrawingWithGraphics.drawStar(g2d);
		FileOutputStream out = new FileOutputStream("workbook.xls");
		wb.write(out);
		out.close();
	}

	private static void drawStar(EscherGraphics2d g2d) {
		g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		for (double i = 0; i < (Math.PI); i += 0.1) {
			g2d.setColor(new Color(((int) (i * 5343062.0))));
			int x1 = ((int) ((Math.cos(i)) * 160.0)) + 160;
			int y1 = ((int) ((Math.sin(i)) * 138.0)) + 138;
			int x2 = ((int) ((-(Math.cos(i))) * 160.0)) + 160;
			int y2 = ((int) ((-(Math.sin(i))) * 138.0)) + 138;
			g2d.setStroke(new BasicStroke(2));
			g2d.drawLine(x1, y1, x2, y2);
		}
		g2d.setFont(new Font("SansSerif", ((Font.BOLD) | (Font.ITALIC)), 20));
		g2d.drawString("EscherGraphics2d", 70, 100);
		g2d.setColor(Color.yellow);
		g2d.fillOval((160 - 20), (138 - 20), 40, 40);
		g2d.setColor(Color.black);
		g2d.fillPolygon(new int[]{ (-10) + 160, 0 + 160, 10 + 160, 0 + 160 }, new int[]{ 0 + 138, 10 + 138, 0 + 138, (-10) + 138 }, 4);
		g2d.drawPolygon(new int[]{ (-160) + 160, 0 + 160, 160 + 160, 0 + 160 }, new int[]{ 0 + 138, 138 + 138, 0 + 138, (-138) + 138 }, 4);
	}
}

