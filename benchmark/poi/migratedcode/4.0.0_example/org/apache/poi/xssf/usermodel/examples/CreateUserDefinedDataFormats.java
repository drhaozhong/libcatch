package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class CreateUserDefinedDataFormats {
	public static void main(String[] args) throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("format sheet");
			CellStyle style;
			DataFormat format = wb.createDataFormat();
			Row row;
			Cell cell;
			short rowNum = 0;
			short colNum = 0;
			row = sheet.createRow(rowNum);
			cell = row.createCell(colNum);
			cell.setCellValue(11111.25);
			style = wb.createCellStyle();
			style.setDataFormat(format.getFormat("0.0"));
			cell.setCellStyle(style);
			row = sheet.createRow((++rowNum));
			cell = row.createCell(colNum);
			cell.setCellValue(11111.25);
			style = wb.createCellStyle();
			style.setDataFormat(format.getFormat("#,##0.0000"));
			cell.setCellStyle(style);
			try (FileOutputStream fileOut = new FileOutputStream("ooxml_dataFormat.xlsx")) {
				wb.write(fileOut);
			}
		}
	}
}

