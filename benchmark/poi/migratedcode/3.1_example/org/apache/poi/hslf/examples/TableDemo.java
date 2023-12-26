package org.apache.poi.hslf.examples;


import java.awt.Color;
import java.awt.Dimension;
import java.awt.Rectangle;
import java.io.FileOutputStream;
import org.apache.poi.hslf.model.Fill;
import org.apache.poi.hslf.model.Line;
import org.apache.poi.hslf.model.Shape;
import org.apache.poi.hslf.model.ShapeGroup;
import org.apache.poi.hslf.model.Sheet;
import org.apache.poi.hslf.model.SimpleShape;
import org.apache.poi.hslf.model.Slide;
import org.apache.poi.hslf.model.Table;
import org.apache.poi.hslf.model.TableCell;
import org.apache.poi.hslf.model.TextBox;
import org.apache.poi.hslf.model.TextRun;
import org.apache.poi.hslf.model.TextShape;
import org.apache.poi.hslf.usermodel.RichTextRun;
import org.apache.poi.hslf.usermodel.SlideShow;


public class TableDemo {
	public static void main(String[] args) throws Exception {
		String[][] txt1 = new String[][]{ new String[]{ "INPUT FILE", "NUMBER OF RECORDS" }, new String[]{ "Item File", "11,559" }, new String[]{ "Vendor File", "502" }, new String[]{ "Purchase History File - # of PO\u2019s\r(12/01/04 - 05/31/06)", "12,852" }, new String[]{ "Purchase History File - # of PO Lines\r(12/01/04 - 05/31/06)", "53,523" }, new String[]{ "Total PO History Spend", "$10,172,038" } };
		SlideShow ppt = new SlideShow();
		Slide slide = ppt.createSlide();
		Table table1 = new Table(6, 2);
		for (int i = 0; i < (txt1.length); i++) {
			for (int j = 0; j < (txt1[i].length); j++) {
				TableCell cell = table1.getCell(i, j);
				cell.setText(txt1[i][j]);
				RichTextRun rt = cell.getTextRun().getRichTextRuns()[0];
				rt.setFontName("Arial");
				rt.setFontSize(10);
				if (i == 0) {
					cell.getFill().setForegroundColor(new Color(227, 227, 227));
				}else {
					rt.setBold(true);
				}
				cell.setVerticalAlignment(TextBox.AnchorMiddle);
				cell.setHorizontalAlignment(TextBox.AlignCenter);
			}
		}
		Line border1 = table1.createBorder();
		border1.setLineColor(Color.black);
		border1.setLineWidth(1.0);
		table1.setAllBorders(border1);
		table1.setColumnWidth(0, 300);
		table1.setColumnWidth(1, 150);
		slide.addShape(table1);
		int pgWidth = ppt.getPageSize().width;
		table1.moveTo(((pgWidth - (table1.getAnchor().width)) / 2), 100);
		String[][] txt2 = new String[][]{ new String[]{ "Data Source" }, new String[]{ "CAS Internal Metrics - Item Master Summary\r" + ("CAS Internal Metrics - Vendor Summary\r" + "CAS Internal Metrics - PO History Summary") } };
		Table table2 = new Table(2, 1);
		for (int i = 0; i < (txt2.length); i++) {
			for (int j = 0; j < (txt2[i].length); j++) {
				TableCell cell = table2.getCell(i, j);
				cell.setText(txt2[i][j]);
				RichTextRun rt = cell.getTextRun().getRichTextRuns()[0];
				rt.setFontSize(10);
				rt.setFontName("Arial");
				if (i == 0) {
					cell.getFill().setForegroundColor(new Color(0, 51, 102));
					rt.setFontColor(Color.white);
					rt.setBold(true);
					rt.setFontSize(14);
					cell.setHorizontalAlignment(TextBox.AlignCenter);
				}else {
					rt.setBullet(true);
					rt.setFontSize(12);
					cell.setHorizontalAlignment(TextBox.AlignLeft);
				}
				cell.setVerticalAlignment(TextBox.AnchorMiddle);
			}
		}
		table2.setColumnWidth(0, 300);
		table2.setRowHeight(0, 30);
		table2.setRowHeight(1, 70);
		Line border2 = table2.createBorder();
		table2.setOutsideBorders(border2);
		slide.addShape(table2);
		table2.moveTo(200, 400);
		FileOutputStream out = new FileOutputStream("hslf-table.ppt");
		ppt.write(out);
		out.close();
	}
}

