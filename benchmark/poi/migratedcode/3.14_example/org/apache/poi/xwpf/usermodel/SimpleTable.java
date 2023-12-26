package org.apache.poi.xwpf.usermodel;


import java.io.FileOutputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.List;
import org.apache.poi.POIXMLDocument;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTHeight;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTRow;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTShd;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTString;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTbl;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTblPr;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTblPrBase;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTc;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTcPr;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTcPrBase;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTrPr;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTTrPrBase;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTVerticalJc;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.STShd;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.STVerticalJc;


public class SimpleTable {
	public static void main(String[] args) throws Exception {
		try {
			SimpleTable.createSimpleTable();
		} catch (Exception e) {
			System.out.println("Error trying to create simple table.");
			throw e;
		}
		try {
			SimpleTable.createStyledTable();
		} catch (Exception e) {
			System.out.println("Error trying to create styled table.");
			throw e;
		}
	}

	public static void createSimpleTable() throws Exception {
		XWPFDocument doc = new XWPFDocument();
		XWPFTable table = doc.createTable(3, 3);
		table.getRow(1).getCell(1).setText("EXAMPLE OF TABLE");
		XWPFParagraph p1 = table.getRow(0).getCell(0).getParagraphs().get(0);
		XWPFRun r1 = p1.createRun();
		r1.setBold(true);
		r1.setText("The quick brown fox");
		r1.setItalic(true);
		r1.setFontFamily("Courier");
		r1.setUnderline(UnderlinePatterns.DOT_DOT_DASH);
		r1.setTextPosition(100);
		table.getRow(2).getCell(2).setText("only text");
		FileOutputStream out = new FileOutputStream("simpleTable.docx");
		doc.write(out);
		out.close();
	}

	public static void createStyledTable() throws Exception {
		XWPFDocument doc = new XWPFDocument();
		int nRows = 6;
		int nCols = 3;
		XWPFTable table = doc.createTable(nRows, nCols);
		CTTblPr tblPr = table.getCTTbl().getTblPr();
		CTString styleStr = tblPr.addNewTblStyle();
		styleStr.setVal("StyledTable");
		List<XWPFTableRow> rows = table.getRows();
		int rowCt = 0;
		int colCt = 0;
		for (XWPFTableRow row : rows) {
			CTTrPr trPr = row.getCtRow().addNewTrPr();
			CTHeight ht = trPr.addNewTrHeight();
			ht.setVal(BigInteger.valueOf(360));
			List<XWPFTableCell> cells = row.getTableCells();
			for (XWPFTableCell cell : cells) {
				CTTcPr tcpr = cell.getCTTc().addNewTcPr();
				CTVerticalJc va = tcpr.addNewVAlign();
				va.setVal(STVerticalJc.CENTER);
				CTShd ctshd = tcpr.addNewShd();
				ctshd.setColor("auto");
				ctshd.setVal(STShd.CLEAR);
				if (rowCt == 0) {
					ctshd.setFill("A7BFDE");
				}else
					if ((rowCt % 2) == 0) {
						ctshd.setFill("D3DFEE");
					}else {
						ctshd.setFill("EDF2F8");
					}

				XWPFParagraph para = cell.getParagraphs().get(0);
				XWPFRun rh = para.createRun();
				if (colCt == (nCols - 1)) {
					rh.setFontSize(10);
					rh.setFontFamily("Courier");
				}
				if (rowCt == 0) {
					rh.setText(("header row, col " + colCt));
					rh.setBold(true);
					para.setAlignment(ParagraphAlignment.CENTER);
				}else
					if ((rowCt % 2) == 0) {
						rh.setText(((("row " + rowCt) + ", col ") + colCt));
						para.setAlignment(ParagraphAlignment.LEFT);
					}else {
						rh.setText(((("row " + rowCt) + ", col ") + colCt));
						para.setAlignment(ParagraphAlignment.LEFT);
					}

				colCt++;
			}
			colCt = 0;
			rowCt++;
		}
		FileOutputStream out = new FileOutputStream("styledTable.docx");
		doc.write(out);
		out.close();
	}
}

