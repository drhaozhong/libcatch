package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class WorkingWithBorders {
	public static void main(String[] args) throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("borders");
			Row row = sheet.createRow(((short) (1)));
			Cell cell = row.createCell(((short) (1)));
			cell.setCellValue(4);
			CellStyle style = wb.createCellStyle();
			style.setBorderBottom(BorderStyle.THIN);
			style.setBottomBorderColor(IndexedColors.BLACK.getIndex());
			style.setBorderLeft(BorderStyle.THIN);
			style.setLeftBorderColor(IndexedColors.GREEN.getIndex());
			style.setBorderRight(BorderStyle.THIN);
			style.setRightBorderColor(IndexedColors.BLUE.getIndex());
			style.setBorderTop(BorderStyle.MEDIUM_DASHED);
			style.setTopBorderColor(IndexedColors.BLACK.getIndex());
			cell.setCellStyle(style);
			try (FileOutputStream fileOut = new FileOutputStream("xssf-borders.xlsx")) {
				wb.write(fileOut);
			}
		}
	}
}

