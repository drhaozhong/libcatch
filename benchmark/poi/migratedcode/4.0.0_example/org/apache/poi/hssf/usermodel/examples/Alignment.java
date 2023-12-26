package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.HorizontalAlignment;


public class Alignment {
	public static void main(String[] args) throws IOException {
		try (HSSFWorkbook wb = new HSSFWorkbook()) {
			HSSFSheet sheet = wb.createSheet("new sheet");
			HSSFRow row = sheet.createRow(2);
			Alignment.createCell(wb, row, 0, HorizontalAlignment.CENTER);
			Alignment.createCell(wb, row, 1, HorizontalAlignment.CENTER_SELECTION);
			Alignment.createCell(wb, row, 2, HorizontalAlignment.FILL);
			Alignment.createCell(wb, row, 3, HorizontalAlignment.GENERAL);
			Alignment.createCell(wb, row, 4, HorizontalAlignment.JUSTIFY);
			Alignment.createCell(wb, row, 5, HorizontalAlignment.LEFT);
			Alignment.createCell(wb, row, 6, HorizontalAlignment.RIGHT);
			try (FileOutputStream fileOut = new FileOutputStream("workbook.xls")) {
				wb.write(fileOut);
			}
		}
	}

	private static void createCell(HSSFWorkbook wb, HSSFRow row, int column, HorizontalAlignment align) {
		HSSFCell cell = row.createCell(column);
		cell.setCellValue("Align It");
		HSSFCellStyle cellStyle = wb.createCellStyle();
		cellStyle.setAlignment(align);
		cell.setCellStyle(cellStyle);
	}
}

