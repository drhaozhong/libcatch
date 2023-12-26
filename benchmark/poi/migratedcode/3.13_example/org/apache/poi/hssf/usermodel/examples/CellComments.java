package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFClientAnchor;
import org.apache.poi.hssf.usermodel.HSSFComment;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFPatriarch;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFShape;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFSimpleShape;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;

import static org.apache.poi.hssf.util.HSSFColor.RED.index;


public class CellComments {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet = wb.createSheet("Cell comments in POI HSSF");
		HSSFPatriarch patr = sheet.createDrawingPatriarch();
		HSSFCell cell1 = sheet.createRow(3).createCell(1);
		cell1.setCellValue(new HSSFRichTextString("Hello, World"));
		HSSFComment comment1 = patr.createComment(new HSSFClientAnchor(0, 0, 0, 0, ((short) (4)), 2, ((short) (6)), 5));
		comment1.setString(new HSSFRichTextString("We can set comments in POI"));
		comment1.setAuthor("Apache Software Foundation");
		cell1.setCellComment(comment1);
		HSSFCell cell2 = sheet.createRow(6).createCell(1);
		cell2.setCellValue(36.6);
		HSSFComment comment2 = patr.createComment(new HSSFClientAnchor(0, 0, 0, 0, ((short) (4)), 8, ((short) (6)), 11));
		comment2.setFillColor(204, 236, 255);
		HSSFRichTextString string = new HSSFRichTextString("Normal body temperature");
		HSSFFont font = wb.createFont();
		font.setFontName("Arial");
		font.setFontHeightInPoints(((short) (10)));
		font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
		font.setColor(index);
		string.applyFont(font);
		comment2.setString(string);
		comment2.setVisible(true);
		comment2.setAuthor("Bill Gates");
		comment2.setRow(6);
		comment2.setColumn(1);
		FileOutputStream out = new FileOutputStream("poi_comment.xls");
		wb.write(out);
		out.close();
	}
}

