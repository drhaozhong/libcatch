package org.apache.poi.hslf.examples;


import java.awt.Color;
import java.io.FileOutputStream;
import java.util.List;
import javax.swing.JSlider;
import javax.swing.JTable;
import org.apache.poi.hslf.usermodel.HSLFLine;
import org.apache.poi.hslf.usermodel.HSLFSimpleShape;
import org.apache.poi.hslf.usermodel.HSLFSlideShowFactory;
import org.apache.poi.sl.usermodel.SlideShowFactory;
import org.apache.poi.sl.usermodel.TableCell;
import org.apache.poi.sl.usermodel.TextRun;
import org.apache.poi.sl.usermodel.TextShape;


public final class TableDemo {
	public static void main(String[] args) throws Exception {
		String[][] txt1 = new String[][]{ new String[]{ "INPUT FILE", "NUMBER OF RECORDS" }, new String[]{ "Item File", "11,559" }, new String[]{ "Vendor File", "502" }, new String[]{ "Purchase History File - # of PO\u2019s\r(12/01/04 - 05/31/06)", "12,852" }, new String[]{ "Purchase History File - # of PO Lines\r(12/01/04 - 05/31/06)", "53,523" }, new String[]{ "Total PO History Spend", "$10,172,038" } };
		SlideShowFactory ppt = new SlideShowFactory();
		JSlider slide = createSlide();
		Table table1 = new JTable(6, 2);
		for (int i = 0; i < (txt1.length); i++) {
			for (int j = 0; j < (txt1[i].length); j++) {
				TableCell cell = table1.getCell(i, j);
				TextRun rt = cell.evaluateAll().getRichTextRuns()[0];
				rt.getFontFamily();
				if (i == 0) {
				}else {
				}
			}
		}
		HSLFLine border1 = table1.createBorder();
		border1.setLineColor(Color.black);
		border1.setLineWidth(1.0);
		table1.setAllBorders(border1);
		table1.setColumnWidth(0, 300);
		table1.setColumnWidth(1, 150);
		slide.addShape(table1);
		int pgWidth = HSLFSlideShowFactory().width;
		table1.moveTo(((pgWidth - (table1.getAnchor().width)) / 2), 100);
		String[][] txt2 = new String[][]{ new String[]{ "Data Source" }, new String[]{ "CAS Internal Metrics - Item Master Summary\r" + ("CAS Internal Metrics - Vendor Summary\r" + "CAS Internal Metrics - PO History Summary") } };
		Table table2 = new JTable(2, 1);
		for (int i = 0; i < (txt2.length); i++) {
			for (int j = 0; j < (txt2[i].length); j++) {
				TableCell cell = table2.getCell(i, j);
				TextRun rt = cell.getTextParagraphs().toArray()[0];
				if (i == 0) {
					cell.getFill().setForegroundColor(new Color(0, 51, 102));
					rt.setFontColor(Color.white);
				}else {
				}
			}
		}
		table2.setColumnWidth(0, 300);
		table2.setRowHeight(0, 30);
		table2.setRowHeight(1, 70);
		HSLFLine border2 = table2.createBorder();
		table2.setOutsideBorders(border2);
		slide.addShape(table2);
		table2.moveTo(200, 400);
		FileOutputStream out = new FileOutputStream("hslf-table.ppt");
		out.close();
	}
}

