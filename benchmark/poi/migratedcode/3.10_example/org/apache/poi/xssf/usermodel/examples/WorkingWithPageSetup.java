package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class WorkingWithPageSetup {
	public static void main(String[] args) throws Exception {
		Workbook wb = new XSSFWorkbook();
		Sheet sheet1 = wb.createSheet("new sheet");
		Sheet sheet2 = wb.createSheet("second sheet");
		Row row1 = sheet1.createRow(0);
		row1.createCell(0).setCellValue(1);
		row1.createCell(1).setCellValue(2);
		row1.createCell(2).setCellValue(3);
		Row row2 = sheet1.createRow(1);
		row2.createCell(1).setCellValue(4);
		row2.createCell(2).setCellValue(5);
		Row row3 = sheet2.createRow(1);
		row3.createCell(0).setCellValue(2.1);
		row3.createCell(4).setCellValue(2.2);
		row3.createCell(5).setCellValue(2.3);
		Row row4 = sheet2.createRow(2);
		row4.createCell(4).setCellValue(2.4);
		row4.createCell(5).setCellValue(2.5);
		wb.setRepeatingRowsAndColumns(0, 0, 2, (-1), (-1));
		wb.setRepeatingRowsAndColumns(1, 4, 5, 1, 2);
		wb.setPrintArea(0, 1, 2, 0, 3);
		FileOutputStream fileOut = new FileOutputStream("xssf-printsetup.xlsx");
		wb.write(fileOut);
		fileOut.close();
	}
}

