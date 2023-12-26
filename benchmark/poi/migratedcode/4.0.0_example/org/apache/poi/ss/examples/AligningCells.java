package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class AligningCells {
	public static void main(String[] args) throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet();
			Row row = sheet.createRow(2);
			row.setHeightInPoints(30);
			for (int i = 0; i < 8; i++) {
				sheet.setColumnWidth(i, (256 * 15));
			}
			AligningCells.createCell(wb, row, 0, HorizontalAlignment.CENTER, VerticalAlignment.BOTTOM);
			AligningCells.createCell(wb, row, 1, HorizontalAlignment.CENTER_SELECTION, VerticalAlignment.BOTTOM);
			AligningCells.createCell(wb, row, 2, HorizontalAlignment.FILL, VerticalAlignment.CENTER);
			AligningCells.createCell(wb, row, 3, HorizontalAlignment.GENERAL, VerticalAlignment.CENTER);
			AligningCells.createCell(wb, row, 4, HorizontalAlignment.JUSTIFY, VerticalAlignment.JUSTIFY);
			AligningCells.createCell(wb, row, 5, HorizontalAlignment.LEFT, VerticalAlignment.TOP);
			AligningCells.createCell(wb, row, 6, HorizontalAlignment.RIGHT, VerticalAlignment.TOP);
			try (OutputStream fileOut = new FileOutputStream("ss-example-align.xlsx")) {
				wb.write(fileOut);
			}
		}
	}

	private static void createCell(Workbook wb, Row row, int column, HorizontalAlignment halign, VerticalAlignment valign) {
		CreationHelper ch = wb.getCreationHelper();
		Cell cell = row.createCell(column);
		cell.setCellValue(ch.createRichTextString("Align It"));
		CellStyle cellStyle = wb.createCellStyle();
		cellStyle.setAlignment(halign);
		cellStyle.setVerticalAlignment(valign);
		cell.setCellStyle(cellStyle);
	}
}

