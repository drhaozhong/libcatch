package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;


public class WorkingWithFonts {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet = wb.createSheet("new sheet");
		HSSFRow row = sheet.createRow(1);
		HSSFFont font = wb.createFont();
		font.setFontHeightInPoints(((short) (24)));
		font.setFontName("Courier New");
		font.setItalic(true);
		font.setStrikeout(true);
		HSSFCellStyle style = wb.createCellStyle();
		style.setFont(font);
		HSSFCell cell = row.createCell(1);
		cell.setCellValue("This is a test of fonts");
		cell.setCellStyle(style);
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
		wb.close();
	}
}

