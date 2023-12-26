package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class CreateCell {
	public static void main(String[] args) throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			CreationHelper creationHelper = wb.getCreationHelper();
			Sheet sheet = wb.createSheet("new sheet");
			Row row = sheet.createRow(((short) (0)));
			Cell cell = row.createCell(((short) (0)));
			cell.setCellValue(1);
			row.createCell(1).setCellValue(1.2);
			row.createCell(2).setCellValue("This is a string cell");
			RichTextString str = creationHelper.createRichTextString("Apache");
			Font font = wb.createFont();
			font.setItalic(true);
			font.setUnderline(Font.U_SINGLE);
			str.applyFont(font);
			row.createCell(3).setCellValue(str);
			row.createCell(4).setCellValue(true);
			row.createCell(5).setCellFormula("SUM(A1:B1)");
			CellStyle style = wb.createCellStyle();
			style.setDataFormat(creationHelper.createDataFormat().getFormat("m/d/yy h:mm"));
			cell = row.createCell(6);
			cell.setCellValue(new Date());
			cell.setCellStyle(style);
			row.createCell(7).setCellFormula("SUM(A1:B1)");
			cell.setCellFormula("HYPERLINK(\"http://google.com\",\"Google\")");
			try (FileOutputStream fileOut = new FileOutputStream("ooxml-cell.xlsx")) {
				wb.write(fileOut);
			}
		}
	}
}

