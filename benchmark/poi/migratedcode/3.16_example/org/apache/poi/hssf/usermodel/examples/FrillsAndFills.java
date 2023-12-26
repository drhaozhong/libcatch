package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.FillPatternType;

import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.AQUA;
import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.ORANGE;


public class FrillsAndFills {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet = wb.createSheet("new sheet");
		HSSFRow row = sheet.createRow(1);
		HSSFCellStyle style = wb.createCellStyle();
		style.setFillBackgroundColor(AQUA.getIndex());
		style.setFillPattern(FillPatternType.BIG_SPOTS);
		HSSFCell cell = row.createCell(1);
		cell.setCellValue("X");
		cell.setCellStyle(style);
		style = wb.createCellStyle();
		style.setFillForegroundColor(ORANGE.getIndex());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		cell = row.createCell(2);
		cell.setCellValue("X");
		cell.setCellStyle(style);
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
		wb.close();
	}
}

