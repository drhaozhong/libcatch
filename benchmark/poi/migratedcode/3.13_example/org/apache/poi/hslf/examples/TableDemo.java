package org.apache.poi.hslf.examples;


import java.awt.Color;
import java.awt.Dimension;
import java.io.FileOutputStream;
import java.util.List;
import org.apache.poi.hslf.usermodel.HSLFFill;
import org.apache.poi.hslf.usermodel.HSLFLine;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSimpleShape;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTable;
import org.apache.poi.hslf.usermodel.HSLFTableCell;
import org.apache.poi.hslf.usermodel.HSLFTextParagraph;
import org.apache.poi.hslf.usermodel.HSLFTextRun;
import org.apache.poi.hslf.usermodel.HSLFTextShape;
import org.apache.poi.sl.usermodel.VerticalAlignment;


public final class TableDemo {
	public static void main(String[] args) throws Exception {
		String[][] txt1 = new String[][]{ new String[]{ "INPUT FILE", "NUMBER OF RECORDS" }, new String[]{ "Item File", "11,559" }, new String[]{ "Vendor File", "502" }, new String[]{ "Purchase History File - # of PO\u2019s\r(12/01/04 - 05/31/06)", "12,852" }, new String[]{ "Purchase History File - # of PO Lines\r(12/01/04 - 05/31/06)", "53,523" }, new String[]{ "Total PO History Spend", "$10,172,038" } };
		HSLFSlideShow ppt = new HSLFSlideShow();
		HSLFSlide slide = ppt.createSlide();
		HSLFTable table1 = new HSLFTable(6, 2);
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
		HSLFLine border1 = createHeader1();
		border1.setLineColor(Color.black);
		border1.setLineWidth(1.0);
		table1.setColumnWidth(0, 300);
		table1.setColumnWidth(1, 150);
		slide.addShape(table1);
		int pgWidth = ppt.getPageSize().width;
		String[][] txt2 = new String[][]{ new String[]{ "Data Source" }, new String[]{ "CAS Internal Metrics - Item Master Summary\r" + ("CAS Internal Metrics - Vendor Summary\r" + "CAS Internal Metrics - PO History Summary") } };
		HSLFTable table2 = new HSLFTable(2, 1);
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
					cell.setHorizontalCentered(false);
				}
				cell.setVerticalAlignment(VerticalAlignment.MIDDLE);
				cell.setText(txt2[i][j]);
			}
		}
		table2.setColumnWidth(0, 300);
		table2.setRowHeight(0, 30);
		table2.setRowHeight(1, 70);
		HSLFLine border2 = createBuilder();
		slide.addShape(table2);
		table2.moveTo(200, 400);
		FileOutputStream out = new FileOutputStream("hslf-table.ppt");
		ppt.write(out);
		out.close();
	}
}

