package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hpsf.CustomProperties;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.ooxml.POIXMLProperties;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class WorkbookProperties {
	public static void main(String[] args) throws IOException {
		XSSFWorkbook workbook = new XSSFWorkbook();
		workbook.createSheet("Workbook Properties");
		CustomProperties props = workbook.getProperties();
		org.apache.poi.POIXMLProperties.CustomProperties ext = getExtendedProperties();
		ext.getUnderlyingProperties().setCompany("Apache Software Foundation");
		ext.getUnderlyingProperties().setTemplate("XSSF");
		POIXMLProperties cust = getCustomProperties();
		addProperty("Author", "John Smith");
		FileOutputStream out = new FileOutputStream("workbook.xlsx");
		workbook.write(out);
		out.close();
		workbook.close();
	}
}

