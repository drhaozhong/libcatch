package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;


public class RepeatingRowsAndColumns {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet1 = wb.createSheet("first sheet");
		wb.createSheet("second sheet");
		wb.createSheet("third sheet");
		HSSFFont boldFont = wb.createFont();
		boldFont.setFontHeightInPoints(((short) (22)));
		boldFont.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
		HSSFCellStyle boldStyle = wb.createCellStyle();
		boldStyle.setFont(boldFont);
		HSSFRow row = sheet1.createRow(1);
		HSSFCell cell = row.createCell(0);
		cell.setCellValue("This quick brown fox");
		cell.setCellStyle(boldStyle);
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
	}
}

