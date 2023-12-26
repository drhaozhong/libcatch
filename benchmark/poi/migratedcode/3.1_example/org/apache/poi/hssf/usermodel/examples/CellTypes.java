package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;


public class CellTypes {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet = wb.createSheet("new sheet");
		HSSFRow row = sheet.createRow(((short) (2)));
		row.createCell(((short) (0))).setCellValue(1.1);
		row.createCell(((short) (1))).setCellValue(new Date());
		row.createCell(((short) (2))).setCellValue("a string");
		row.createCell(((short) (3))).setCellValue(true);
		row.createCell(((short) (4))).setCellType(HSSFCell.CELL_TYPE_ERROR);
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
	}
}
