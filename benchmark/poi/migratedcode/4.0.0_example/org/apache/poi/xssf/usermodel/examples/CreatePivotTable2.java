package org.apache.poi.xssf.usermodel.examples;


import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataConsolidateFunction;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFPivotTable;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class CreatePivotTable2 {
	public static void main(String[] args) throws FileNotFoundException, IOException, InvalidFormatException {
		try (XSSFWorkbook wb = new XSSFWorkbook()) {
			XSSFSheet sheet = wb.createSheet();
			CreatePivotTable2.setCellData(sheet);
			AreaReference source = new AreaReference("A1:E7", SpreadsheetVersion.EXCEL2007);
			CellReference position = new CellReference("H1");
			XSSFPivotTable pivotTable = sheet.createPivotTable(source, position);
			pivotTable.addRowLabel(0);
			pivotTable.addColumnLabel(DataConsolidateFunction.SUM, 1, "Values", "#,##0.00");
			pivotTable.addColLabel(3, "DD.MM.YYYY");
			pivotTable.addReportFilter(4);
			try (FileOutputStream fileOut = new FileOutputStream("ooxml-pivottable2.xlsx")) {
				wb.write(fileOut);
			}
		}
	}

	public static void setCellData(XSSFSheet sheet) {
		Calendar cal1 = Calendar.getInstance();
		cal1.set(2017, 0, 1, 0, 0, 0);
		Calendar cal2 = Calendar.getInstance();
		cal2.set(2017, 1, 1, 0, 0, 0);
		Row row1 = sheet.createRow(0);
		Cell cell11 = row1.createCell(0);
		cell11.setCellValue("Names");
		Cell cell12 = row1.createCell(1);
		cell12.setCellValue("Values");
		Cell cell13 = row1.createCell(2);
		cell13.setCellValue("%");
		Cell cell14 = row1.createCell(3);
		cell14.setCellValue("Month");
		Cell cell15 = row1.createCell(4);
		cell15.setCellValue("No");
		CellStyle csDbl = sheet.getWorkbook().createCellStyle();
		DataFormat dfDbl = sheet.getWorkbook().createDataFormat();
		csDbl.setDataFormat(dfDbl.getFormat("#,##0.00"));
		CellStyle csDt = sheet.getWorkbook().createCellStyle();
		DataFormat dfDt = sheet.getWorkbook().createDataFormat();
		csDt.setDataFormat(dfDt.getFormat("dd/MM/yyyy"));
		CreatePivotTable2.setDataRow(sheet, 1, "Jane", 1120.5, 100, cal1.getTime(), 1, csDbl, csDt);
		CreatePivotTable2.setDataRow(sheet, 2, "Jane", 1453.2, 95, cal2.getTime(), 2, csDbl, csDt);
		CreatePivotTable2.setDataRow(sheet, 3, "Tarzan", 1869.8, 88, cal1.getTime(), 1, csDbl, csDt);
		CreatePivotTable2.setDataRow(sheet, 4, "Tarzan", 1536.2, 92, cal2.getTime(), 2, csDbl, csDt);
		CreatePivotTable2.setDataRow(sheet, 5, "Terk", 1624.1, 75, cal1.getTime(), 1, csDbl, csDt);
		CreatePivotTable2.setDataRow(sheet, 6, "Terk", 1569.3, 82, cal2.getTime(), 2, csDbl, csDt);
		sheet.autoSizeColumn(3);
	}

	public static void setDataRow(XSSFSheet sheet, int rowNum, String name, double v1, int v2, Date dt, int no, CellStyle csDbl, CellStyle csDt) {
		Row row = sheet.createRow(rowNum);
		Cell c1 = row.createCell(0);
		c1.setCellValue(name);
		Cell c2 = row.createCell(1);
		c2.setCellValue(v1);
		c2.setCellStyle(csDbl);
		Cell c3 = row.createCell(2);
		c3.setCellValue(v2);
		Cell c4 = row.createCell(3);
		c4.setCellValue(dt);
		c4.setCellStyle(csDt);
		Cell c5 = row.createCell(4);
		c5.setCellValue(no);
	}
}

