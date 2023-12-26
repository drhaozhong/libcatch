package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.io.IOException;
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
		Workbook wb = new XSSFWorkbook();
		Sheet sheet = wb.createSheet();
		Row row = sheet.createRow(((short) (2)));
		row.setHeightInPoints(30);
		for (int i = 0; i < 8; i++) {
			sheet.setColumnWidth(i, (256 * 15));
		}
		AligningCells.createCell(wb, row, ((short) (0)), HorizontalAlignment.CENTER, VerticalAlignment.BOTTOM);
		AligningCells.createCell(wb, row, ((short) (1)), HorizontalAlignment.CENTER_SELECTION, VerticalAlignment.BOTTOM);
		AligningCells.createCell(wb, row, ((short) (2)), HorizontalAlignment.FILL, VerticalAlignment.CENTER);
		AligningCells.createCell(wb, row, ((short) (3)), HorizontalAlignment.GENERAL, VerticalAlignment.CENTER);
		AligningCells.createCell(wb, row, ((short) (4)), HorizontalAlignment.JUSTIFY, VerticalAlignment.JUSTIFY);
		AligningCells.createCell(wb, row, ((short) (5)), HorizontalAlignment.LEFT, VerticalAlignment.TOP);
		AligningCells.createCell(wb, row, ((short) (6)), HorizontalAlignment.RIGHT, VerticalAlignment.TOP);
		FileOutputStream fileOut = new FileOutputStream("ss-example-align.xlsx");
		wb.write(fileOut);
		fileOut.close();
		wb.close();
	}

	private static void createCell(Workbook wb, Row row, short column, HorizontalAlignment halign, VerticalAlignment valign) {
		CreationHelper ch = wb.getCreationHelper();
		Cell cell = row.createCell(column);
		cell.setCellValue(ch.createRichTextString("Align It"));
		CellStyle cellStyle = wb.createCellStyle();
		cellStyle.setAlignment(halign);
		cellStyle.setVerticalAlignment(valign);
		cell.setCellStyle(cellStyle);
	}
}

