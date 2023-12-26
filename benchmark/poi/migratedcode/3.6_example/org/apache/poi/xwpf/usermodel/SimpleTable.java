package org.apache.poi.xwpf.usermodel;


import java.io.FileOutputStream;
import org.apache.poi.POIXMLDocument;


public class SimpleTable {
	public static void main(String[] args) throws Exception {
		XWPFDocument doc = new XWPFDocument();
		XWPFTable table = doc.createTable(3, 3);
		table.getRow(1).getCell(1).setText("EXAMPLE OF TABLE");
		XWPFParagraph p1 = doc.createParagraph();
		XWPFRun r1 = p1.createRun();
		r1.setBold(true);
		r1.setText("The quick brown fox");
		r1.setBold(true);
		r1.setFontFamily("Courier");
		r1.setUnderline(UnderlinePatterns.DOT_DOT_DASH);
		r1.setTextPosition(100);
		table.getRow(0).getCell(0).setParagraph(p1);
		table.getRow(2).getCell(2).setText("only text");
		FileOutputStream out = new FileOutputStream("simpleTable.docx");
		doc.write(out);
		out.close();
	}
}

