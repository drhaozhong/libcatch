package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class FillsAndColors {
	public static void main(String[] args) throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("new sheet");
			Row row = sheet.createRow(1);
			CellStyle style = wb.createCellStyle();
			style.setFillBackgroundColor(IndexedColors.AQUA.getIndex());
			style.setFillPattern(FillPatternType.BIG_SPOTS);
			Cell cell = row.createCell(1);
			cell.setCellValue(new XSSFRichTextString("X"));
			cell.setCellStyle(style);
			style = wb.createCellStyle();
			style.setFillForegroundColor(IndexedColors.ORANGE.getIndex());
			style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			cell = row.createCell(2);
			cell.setCellValue(new XSSFRichTextString("X"));
			cell.setCellStyle(style);
			try (FileOutputStream fileOut = new FileOutputStream("fill_colors.xlsx")) {
				wb.write(fileOut);
			}
		}
	}
}

