package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.ooxml.POIXMLProperties;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openxmlformats.schemas.officeDocument.x2006.extendedProperties.CTProperties;


public class WorkbookProperties {
	public static void main(String[] args) throws IOException {
		try (XSSFWorkbook workbook = new XSSFWorkbook()) {
			workbook.createSheet("Workbook Properties");
			POIXMLProperties props = workbook.getProperties();
			POIXMLProperties.ExtendedProperties ext = props.getExtendedProperties();
			ext.getUnderlyingProperties().setCompany("Apache Software Foundation");
			ext.getUnderlyingProperties().setTemplate("XSSF");
			POIXMLProperties.CustomProperties cust = props.getCustomProperties();
			cust.addProperty("Author", "John Smith");
			cust.addProperty("Year", 2009);
			cust.addProperty("Price", 45.5);
			cust.addProperty("Available", true);
			try (FileOutputStream out = new FileOutputStream("workbook.xlsx")) {
				workbook.write(out);
			}
		}
	}
}

