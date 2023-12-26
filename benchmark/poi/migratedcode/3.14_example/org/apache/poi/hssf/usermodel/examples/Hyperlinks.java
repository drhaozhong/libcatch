package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFHyperlink;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;

import static org.apache.poi.hssf.util.HSSFColor.BLUE.index;


public class Hyperlinks {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFCellStyle hlink_style = wb.createCellStyle();
		HSSFFont hlink_font = wb.createFont();
		hlink_font.setUnderline(HSSFFont.U_SINGLE);
		hlink_font.setColor(index);
		hlink_style.setFont(hlink_font);
		HSSFCell cell;
		HSSFSheet sheet = wb.createSheet("Hyperlinks");
		cell = sheet.createRow(0).createCell(0);
		cell.setCellValue("URL Link");
		HSSFHyperlink link = new HSSFHyperlink(HSSFHyperlink.LINK_URL);
		link.setAddress("http://poi.apache.org/");
		cell.setHyperlink(link);
		cell.setCellStyle(hlink_style);
		cell = sheet.createRow(1).createCell(0);
		cell.setCellValue("File Link");
		link = new HSSFHyperlink(HSSFHyperlink.LINK_FILE);
		link.setAddress("link1.xls");
		cell.setHyperlink(link);
		cell.setCellStyle(hlink_style);
		cell = sheet.createRow(2).createCell(0);
		cell.setCellValue("Email Link");
		link = new HSSFHyperlink(HSSFHyperlink.LINK_EMAIL);
		link.setAddress("mailto:poi@apache.org?subject=Hyperlinks");
		cell.setHyperlink(link);
		cell.setCellStyle(hlink_style);
		HSSFSheet sheet2 = wb.createSheet("Target Sheet");
		sheet2.createRow(0).createCell(0).setCellValue("Target Cell");
		cell = sheet.createRow(3).createCell(0);
		cell.setCellValue("Worksheet Link");
		link = new HSSFHyperlink(HSSFHyperlink.LINK_DOCUMENT);
		link.setAddress("'Target Sheet'!A1");
		cell.setHyperlink(link);
		cell.setCellStyle(hlink_style);
		FileOutputStream out = new FileOutputStream("hssf-links.xls");
		wb.write(out);
		out.close();
	}
}

