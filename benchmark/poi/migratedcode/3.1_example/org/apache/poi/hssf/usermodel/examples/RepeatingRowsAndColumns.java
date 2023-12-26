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
		HSSFSheet sheet2 = wb.createSheet("second sheet");
		HSSFSheet sheet3 = wb.createSheet("third sheet");
		HSSFFont boldFont = wb.createFont();
		boldFont.setFontHeightInPoints(((short) (22)));
		boldFont.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
		HSSFCellStyle boldStyle = wb.createCellStyle();
		boldStyle.setFont(boldFont);
		HSSFRow row = sheet1.createRow(((short) (1)));
		HSSFCell cell = row.createCell(((short) (0)));
		cell.setCellValue("This quick brown fox");
		cell.setCellStyle(boldStyle);
		wb.setRepeatingRowsAndColumns(0, 0, 2, (-1), (-1));
		wb.setRepeatingRowsAndColumns(1, (-1), (-1), 0, 2);
		wb.setRepeatingRowsAndColumns(2, 4, 5, 1, 2);
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
	}
}

