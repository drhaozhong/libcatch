package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class MergingCells {
	public static void main(String[] args) throws Exception {
		Workbook wb = new XSSFWorkbook();
		Sheet sheet = wb.createSheet("new sheet");
		Row row = sheet.createRow(((short) (1)));
		Cell cell = row.createCell(((short) (1)));
		cell.setCellValue(new XSSFRichTextString("This is a test of merging"));
		sheet.addMergedRegion(new CellRangeAddress(1, 1, 1, 2));
		FileOutputStream fileOut = new FileOutputStream("merging_cells.xlsx");
		wb.write(fileOut);
		fileOut.close();
	}
}

