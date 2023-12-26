package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;


public class Alignment {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet = wb.createSheet("new sheet");
		HSSFRow row = sheet.createRow(2);
		Alignment.createCell(wb, row, 0, HSSFCellStyle.ALIGN_CENTER);
		Alignment.createCell(wb, row, 1, HSSFCellStyle.ALIGN_CENTER_SELECTION);
		Alignment.createCell(wb, row, 2, HSSFCellStyle.ALIGN_FILL);
		Alignment.createCell(wb, row, 3, HSSFCellStyle.ALIGN_GENERAL);
		Alignment.createCell(wb, row, 4, HSSFCellStyle.ALIGN_JUSTIFY);
		Alignment.createCell(wb, row, 5, HSSFCellStyle.ALIGN_LEFT);
		Alignment.createCell(wb, row, 6, HSSFCellStyle.ALIGN_RIGHT);
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
	}

	private static void createCell(HSSFWorkbook wb, HSSFRow row, int column, int align) {
		HSSFCell cell = row.createCell(column);
		cell.setCellValue("Align It");
		HSSFCellStyle cellStyle = wb.createCellStyle();
		cellStyle.setAlignment(((short) (align)));
		cell.setCellStyle(cellStyle);
	}
}

