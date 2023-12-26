package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.BorderStyle;

import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.BLACK;
import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.BLUE;
import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.GREEN;
import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.ORANGE;


public class Borders {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet = wb.createSheet("new sheet");
		HSSFRow row = sheet.createRow(1);
		HSSFCell cell = row.createCell(1);
		cell.setCellValue(4);
		HSSFCellStyle style = wb.createCellStyle();
		style.setBorderBottom(BorderStyle.THIN);
		style.setBottomBorderColor(BLACK.getIndex());
		style.setBorderLeft(BorderStyle.THIN);
		style.setLeftBorderColor(GREEN.getIndex());
		style.setBorderRight(BorderStyle.THIN);
		style.setRightBorderColor(BLUE.getIndex());
		style.setBorderTop(BorderStyle.MEDIUM_DASHED);
		style.setTopBorderColor(ORANGE.getIndex());
		cell.setCellStyle(style);
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
		wb.close();
	}
}

