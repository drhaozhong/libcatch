package org.apache.poi.hslf.examples;


import java.awt.Color;
import java.awt.Dimension;
import java.awt.geom.Rectangle2D;
import java.awt.geom.RectangularShape;
import java.io.FileOutputStream;
import java.util.List;
import org.apache.poi.hslf.usermodel.HSLFFill;
import org.apache.poi.hslf.usermodel.HSLFGroupShape;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTable;
import org.apache.poi.hslf.usermodel.HSLFTableCell;
import org.apache.poi.hslf.usermodel.HSLFTextParagraph;
import org.apache.poi.hslf.usermodel.HSLFTextRun;
import org.apache.poi.hslf.usermodel.HSLFTextShape;
import org.apache.poi.sl.draw.DrawTableShape;
import org.apache.poi.sl.usermodel.TextParagraph;
import org.apache.poi.sl.usermodel.VerticalAlignment;

import static org.apache.poi.sl.usermodel.TextParagraph.TextAlign.LEFT;


public final class TableDemo {
	public static void main(String[] args) throws Exception {
		String[][] txt1 = new String[][]{ new String[]{ "INPUT FILE", "NUMBER OF RECORDS" }, new String[]{ "Item File", "11,559" }, new String[]{ "Vendor File", "502" }, new String[]{ "Purchase History File - # of PO\u2019s\r(12/01/04 - 05/31/06)", "12,852" }, new String[]{ "Purchase History File - # of PO Lines\r(12/01/04 - 05/31/06)", "53,523" }, new String[]{ "Total PO History Spend", "$10,172,038" } };
		HSLFSlideShow ppt = new HSLFSlideShow();
		HSLFSlide slide = ppt.createSlide();
		HSLFTable table1 = slide.createTable(6, 2);
		for (int i = 0; i < (txt1.length); i++) {
			for (int j = 0; j < (txt1[i].length); j++) {
				HSLFTableCell cell = table1.getCell(i, j);
				HSLFTextRun rt = cell.getTextParagraphs().get(0).getTextRuns().get(0);
				rt.setFontFamily("Arial");
				rt.setFontSize(10.0);
				if (i == 0) {
					cell.getFill().setForegroundColor(new Color(227, 227, 227));
				}else {
					rt.setBold(true);
				}
				cell.setVerticalAlignment(VerticalAlignment.MIDDLE);
				cell.setHorizontalCentered(true);
				cell.setText(txt1[i][j]);
			}
		}
		DrawTableShape dts1 = new DrawTableShape(table1);
		dts1.setAllBorders(1.0, Color.black);
		table1.setColumnWidth(0, 300);
		table1.setColumnWidth(1, 150);
		int pgWidth = ppt.getPageSize().width;
		table1.moveTo(((pgWidth - (table1.getAnchor().getWidth())) / 2.0), 100.0);
		String[][] txt2 = new String[][]{ new String[]{ "Data Source" }, new String[]{ "CAS Internal Metrics - Item Master Summary\r" + ("CAS Internal Metrics - Vendor Summary\r" + "CAS Internal Metrics - PO History Summary") } };
		HSLFTable table2 = slide.createTable(2, 1);
		for (int i = 0; i < (txt2.length); i++) {
			for (int j = 0; j < (txt2[i].length); j++) {
				HSLFTableCell cell = table2.getCell(i, j);
				HSLFTextRun rt = cell.getTextParagraphs().get(0).getTextRuns().get(0);
				rt.setFontSize(10.0);
				rt.setFontFamily("Arial");
				if (i == 0) {
					cell.getFill().setForegroundColor(new Color(0, 51, 102));
					rt.setFontColor(Color.white);
					rt.setBold(true);
					rt.setFontSize(14.0);
					cell.setHorizontalCentered(true);
				}else {
					rt.getTextParagraph().setBullet(true);
					rt.setFontSize(12.0);
					rt.getTextParagraph().setTextAlign(LEFT);
					cell.setHorizontalCentered(false);
				}
				cell.setVerticalAlignment(VerticalAlignment.MIDDLE);
				cell.setText(txt2[i][j]);
			}
		}
		table2.setColumnWidth(0, 300);
		table2.setRowHeight(0, 30);
		table2.setRowHeight(1, 70);
		DrawTableShape dts2 = new DrawTableShape(table2);
		dts2.setOutsideBorders(Color.black, 1.0);
		table2.moveTo(200, 400);
		FileOutputStream out = new FileOutputStream("hslf-table.ppt");
		ppt.write(out);
		out.close();
		ppt.close();
	}
}

