package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class AligningCells {
	public static void main(String[] args) throws IOException {
		Workbook wb = new XSSFWorkbook();
		Sheet sheet = wb.createSheet();
		Row row = sheet.createRow(((short) (2)));
		row.setHeightInPoints(30);
		for (int i = 0; i < 8; i++) {
			sheet.setColumnWidth(i, (256 * 15));
		}
		AligningCells.createCell(wb, row, ((short) (0)), XSSFCellStyle.ALIGN_CENTER, XSSFCellStyle.VERTICAL_BOTTOM);
		AligningCells.createCell(wb, row, ((short) (1)), XSSFCellStyle.ALIGN_CENTER_SELECTION, XSSFCellStyle.VERTICAL_BOTTOM);
		AligningCells.createCell(wb, row, ((short) (2)), XSSFCellStyle.ALIGN_FILL, XSSFCellStyle.VERTICAL_CENTER);
		AligningCells.createCell(wb, row, ((short) (3)), XSSFCellStyle.ALIGN_GENERAL, XSSFCellStyle.VERTICAL_CENTER);
		AligningCells.createCell(wb, row, ((short) (4)), XSSFCellStyle.ALIGN_JUSTIFY, XSSFCellStyle.VERTICAL_JUSTIFY);
		AligningCells.createCell(wb, row, ((short) (5)), XSSFCellStyle.ALIGN_LEFT, XSSFCellStyle.VERTICAL_TOP);
		AligningCells.createCell(wb, row, ((short) (6)), XSSFCellStyle.ALIGN_RIGHT, XSSFCellStyle.VERTICAL_TOP);
		FileOutputStream fileOut = new FileOutputStream("xssf-align.xlsx");
		wb.write(fileOut);
		fileOut.close();
	}

	private static void createCell(Workbook wb, Row row, short column, short halign, short valign) {
		Cell cell = row.createCell(column);
		cell.setCellValue(new XSSFRichTextString("Align It"));
		CellStyle cellStyle = wb.createCellStyle();
		cellStyle.setAlignment(halign);
		cellStyle.setVerticalAlignment(valign);
		cell.setCellStyle(cellStyle);
	}
}

