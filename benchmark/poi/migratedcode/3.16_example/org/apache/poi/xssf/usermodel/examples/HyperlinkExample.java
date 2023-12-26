package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class HyperlinkExample {
	public static void main(String[] args) throws IOException {
		Workbook wb = new XSSFWorkbook();
		CreationHelper createHelper = wb.getCreationHelper();
		CellStyle hlink_style = wb.createCellStyle();
		Font hlink_font = wb.createFont();
		hlink_font.setUnderline(Font.U_SINGLE);
		hlink_font.setColor(IndexedColors.BLUE.getIndex());
		hlink_style.setFont(hlink_font);
		Cell cell;
		Sheet sheet = wb.createSheet("Hyperlinks");
		cell = sheet.createRow(0).createCell(0);
		cell.setCellValue("URL Link");
		Hyperlink link = createHelper.createHyperlink(HyperlinkType.URL);
		link.setAddress("http://poi.apache.org/");
		cell.setHyperlink(link);
		cell.setCellStyle(hlink_style);
		cell = sheet.createRow(1).createCell(0);
		cell.setCellValue("File Link");
		link = createHelper.createHyperlink(HyperlinkType.FILE);
		link.setAddress("link1.xls");
		cell.setHyperlink(link);
		cell.setCellStyle(hlink_style);
		cell = sheet.createRow(2).createCell(0);
		cell.setCellValue("Email Link");
		link = createHelper.createHyperlink(HyperlinkType.EMAIL);
		link.setAddress("mailto:poi@apache.org?subject=Hyperlinks");
		cell.setHyperlink(link);
		cell.setCellStyle(hlink_style);
		Sheet sheet2 = wb.createSheet("Target Sheet");
		sheet2.createRow(0).createCell(0).setCellValue("Target Cell");
		cell = sheet.createRow(3).createCell(0);
		cell.setCellValue("Worksheet Link");
		Hyperlink link2 = createHelper.createHyperlink(HyperlinkType.DOCUMENT);
		link2.setAddress("'Target Sheet'!A1");
		cell.setHyperlink(link2);
		cell.setCellStyle(hlink_style);
		FileOutputStream out = new FileOutputStream("hyperinks.xlsx");
		wb.write(out);
		out.close();
		wb.close();
	}
}

