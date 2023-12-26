package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
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
		AligningCells.createCell(wb, row, ((short) (0)), CellStyle.ALIGN_CENTER, CellStyle.VERTICAL_BOTTOM);
		AligningCells.createCell(wb, row, ((short) (1)), CellStyle.ALIGN_CENTER_SELECTION, CellStyle.VERTICAL_BOTTOM);
		AligningCells.createCell(wb, row, ((short) (2)), CellStyle.ALIGN_FILL, CellStyle.VERTICAL_CENTER);
		AligningCells.createCell(wb, row, ((short) (3)), CellStyle.ALIGN_GENERAL, CellStyle.VERTICAL_CENTER);
		AligningCells.createCell(wb, row, ((short) (4)), CellStyle.ALIGN_JUSTIFY, CellStyle.VERTICAL_JUSTIFY);
		AligningCells.createCell(wb, row, ((short) (5)), CellStyle.ALIGN_LEFT, CellStyle.VERTICAL_TOP);
		AligningCells.createCell(wb, row, ((short) (6)), CellStyle.ALIGN_RIGHT, CellStyle.VERTICAL_TOP);
		FileOutputStream fileOut = new FileOutputStream("ss-example-align.xlsx");
		wb.write(fileOut);
		fileOut.close();
		wb.close();
	}

	private static void createCell(Workbook wb, Row row, short column, short halign, short valign) {
		CreationHelper ch = wb.getCreationHelper();
		Cell cell = row.createCell(column);
		cell.setCellValue(ch.createRichTextString("Align It"));
		CellStyle cellStyle = wb.createCellStyle();
		cellStyle.setAlignment(halign);
		cellStyle.setVerticalAlignment(valign);
		cell.setCellStyle(cellStyle);
	}
}

