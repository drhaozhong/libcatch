package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class ShiftRows {
	public static void main(String[] args) throws Exception {
		Workbook wb = new XSSFWorkbook();
		Sheet sheet = wb.createSheet("Sheet1");
		Row row1 = sheet.createRow(1);
		row1.createCell(0).setCellValue(1);
		Row row2 = sheet.createRow(4);
		row2.createCell(1).setCellValue(2);
		Row row3 = sheet.createRow(5);
		row3.createCell(2).setCellValue(3);
		Row row4 = sheet.createRow(6);
		row4.createCell(3).setCellValue(4);
		Row row5 = sheet.createRow(9);
		row5.createCell(4).setCellValue(5);
		sheet.shiftRows(5, 10, (-4));
		FileOutputStream fileOut = new FileOutputStream("shiftRows.xlsx");
		wb.write(fileOut);
		fileOut.close();
	}
}

