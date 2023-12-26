package org.apache.poi.hssf.usermodel.examples;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.CellType;


public class ReadWriteWorkbook {
	public static void main(String[] args) throws IOException {
		try (FileInputStream fileIn = new FileInputStream("workbook.xls")) {
			POIFSFileSystem fs = new POIFSFileSystem(fileIn);
			HSSFWorkbook wb = new HSSFWorkbook(fs);
			HSSFSheet sheet = wb.getSheetAt(0);
			HSSFRow row = sheet.getRow(2);
			if (row == null)
				row = sheet.createRow(2);

			HSSFCell cell = row.getCell(3);
			if (cell == null)
				cell = row.createCell(3);

			cell.setCellType(CellType.STRING);
			cell.setCellValue("a test");
			try (FileOutputStream fileOut = new FileOutputStream("workbookout.xls")) {
				wb.write(fileOut);
			}
		}
	}
}

