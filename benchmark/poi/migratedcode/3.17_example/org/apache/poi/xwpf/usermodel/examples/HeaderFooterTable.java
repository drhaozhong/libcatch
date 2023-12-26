package org.apache.poi.xwpf.usermodel.examples;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.wp.usermodel.HeaderFooterType;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFFooter;
import org.apache.poi.xwpf.usermodel.XWPFHeader;
import org.apache.poi.xwpf.usermodel.XWPFHeaderFooter;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTbl;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTblGrid;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTblGridBase;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTblGridCol;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTblLayoutType;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTblPr;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTblPrBase;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.STTblLayoutType;


public class HeaderFooterTable {
	public static void main(String[] args) throws IOException {
		XWPFDocument doc = new XWPFDocument();
		XWPFHeader hdr = doc.createHeader(HeaderFooterType.DEFAULT);
		XWPFTable tbl = hdr.createTable(1, 3);
		int pad = ((int) (0.1 * 1440));
		tbl.setCellMargins(pad, pad, pad, pad);
		tbl.setWidth(((int) (6.5 * 1440)));
		CTTbl ctTbl = tbl.getCTTbl();
		CTTblPr ctTblPr = ctTbl.addNewTblPr();
		CTTblLayoutType layoutType = ctTblPr.addNewTblLayout();
		layoutType.setType(STTblLayoutType.FIXED);
		BigInteger w = new BigInteger("3120");
		CTTblGrid grid = ctTbl.addNewTblGrid();
		for (int i = 0; i < 3; i++) {
			CTTblGridCol gridCol = grid.addNewGridCol();
			gridCol.setW(w);
		}
		XWPFTableRow row = tbl.getRow(0);
		XWPFTableCell cell = row.getCell(0);
		XWPFParagraph p = cell.getParagraphArray(0);
		XWPFRun r = p.createRun();
		r.setText("header left cell");
		cell = row.getCell(1);
		p = cell.getParagraphArray(0);
		r = p.createRun();
		r.setText("header center cell");
		cell = row.getCell(2);
		p = cell.getParagraphArray(0);
		r = p.createRun();
		r.setText("header right cell");
		XWPFFooter ftr = doc.createFooter(HeaderFooterType.DEFAULT);
		p = ftr.createParagraph();
		r = p.createRun();
		r.setText("footer text");
		OutputStream os = new FileOutputStream(new File("headertable.docx"));
		doc.write(os);
		doc.close();
	}
}

