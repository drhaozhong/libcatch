package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;

import static org.apache.poi.hssf.util.HSSFColor.AQUA.index;


public class FrillsAndFills {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet = wb.createSheet("new sheet");
		HSSFRow row = sheet.createRow(((short) (1)));
		HSSFCellStyle style = wb.createCellStyle();
		style.setFillBackgroundColor(index);
		style.setFillPattern(HSSFCellStyle.BIG_SPOTS);
		HSSFCell cell = row.createCell(((short) (1)));
		cell.setCellValue("X");
		cell.setCellStyle(style);
		style = wb.createCellStyle();
		style.setFillForegroundColor(HSSFColor.ORANGE.index);
		style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		cell = row.createCell(((short) (2)));
		cell.setCellValue("X");
		cell.setCellStyle(style);
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
	}
}

