package org.apache.poi.xslf.usermodel;


import java.awt.Color;
import java.awt.geom.Rectangle2D;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.POIXMLDocument;


public class Tutorial4 {
	public static void main(String[] args) throws IOException {
		XMLSlideShow ppt = new XMLSlideShow();
		XSLFSlide slide = ppt.createSlide();
		XSLFTable tbl = slide.createTable();
		tbl.setAnchor(new Rectangle2D.Double(50, 50, 450, 300));
		int numColumns = 3;
		int numRows = 5;
		XSLFTableRow headerRow = tbl.addRow();
		headerRow.setHeight(50);
		for (int i = 0; i < numColumns; i++) {
			XSLFTableCell th = headerRow.addCell();
			XSLFTextParagraph p = th.addNewTextParagraph();
			p.setTextAlign(TextAlign.CENTER);
			XSLFTextRun r = p.addNewTextRun();
			r.setText(("Header " + (i + 1)));
			r.setBold(true);
			r.setFontColor(Color.white);
			th.setFillColor(new Color(79, 129, 189));
			th.setBorderBottom(2);
			th.setBorderBottomColor(Color.white);
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
	}
}

