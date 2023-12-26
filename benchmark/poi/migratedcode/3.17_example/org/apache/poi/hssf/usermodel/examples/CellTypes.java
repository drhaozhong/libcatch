package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CellType;


public class CellTypes {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		try {
			HSSFSheet sheet = wb.createSheet("new sheet");
			HSSFRow row = sheet.createRow(2);
			row.createCell(0).setCellValue(1.1);
			row.createCell(1).setCellValue(new Date());
			row.createCell(2).setCellValue("a string");
			row.createCell(3).setCellValue(true);
			row.createCell(4).setCellType(CellType.ERROR);
			FileOutputStream fileOut = new FileOutputStream("workbook.xls");
			wb.write(fileOut);
			fileOut.close();
		} finally {
			wb.close();
		}
	}
}

