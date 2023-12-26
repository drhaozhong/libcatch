package org.apache.poi.xslf.usermodel;


import java.awt.Color;
import java.awt.Rectangle;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.sl.usermodel.TableCell;
import org.apache.poi.sl.usermodel.TextParagraph;

import static org.apache.poi.sl.usermodel.TableCell.BorderEdge.bottom;
import static org.apache.poi.sl.usermodel.TextParagraph.TextAlign.CENTER;


public class Tutorial4 {
	public static void main(String[] args) throws IOException {
		XMLSlideShow ppt = new XMLSlideShow();
		XSLFSlide slide = ppt.createSlide();
		XSLFTable tbl = slide.createTable();
		tbl.setAnchor(new Rectangle(50, 50, 450, 300));
		int numColumns = 3;
		int numRows = 5;
		XSLFTableRow headerRow = tbl.addRow();
		headerRow.setHeight(50);
		for (int i = 0; i < numColumns; i++) {
			XSLFTableCell th = headerRow.addCell();
			XSLFTextParagraph p = th.addNewTextParagraph();
			p.setTextAlign(CENTER);
			XSLFTextRun r = p.addNewTextRun();
			r.setText(("Header " + (i + 1)));
			r.setBold(true);
			r.setFontColor(Color.white);
			th.setFillColor(new Color(79, 129, 189));
			th.setBorderWidth(bottom, 2.0);
			th.setBorderColor(bottom, Color.white);
			tbl.setColumnWidth(i, 150);
		}
		for (int rownum = 0; rownum < numRows; rownum++) {
			XSLFTableRow tr = tbl.addRow();
			tr.setHeight(50);
			for (int i = 0; i < numColumns; i++) {
				XSLFTableCell cell = tr.addCell();
				XSLFTextParagraph p = cell.addNewTextParagraph();
				XSLFTextRun r = p.addNewTextRun();
				r.setText(("Cell " + (i + 1)));
				if ((rownum % 2) == 0)
					cell.setFillColor(new Color(208, 216, 232));
				else
					cell.setFillColor(new Color(233, 247, 244));

			}
		}
		FileOutputStream out = new FileOutputStream("table.pptx");
		ppt.write(out);
		out.close();
		ppt.close();
	}
}

