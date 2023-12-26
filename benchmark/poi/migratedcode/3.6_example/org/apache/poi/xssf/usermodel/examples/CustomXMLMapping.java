package org.apache.poi.xssf.usermodel.examples;


import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collection;
import org.apache.poi.xssf.extractor.XSSFExportToXml;
import org.apache.poi.xssf.usermodel.XSSFMap;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class CustomXMLMapping {
	public static void main(String[] args) throws Exception {
		XSSFWorkbook wb = new XSSFWorkbook(args[0]);
		for (XSSFMap map : wb.getCustomXMLMappings()) {
			XSSFExportToXml exporter = new XSSFExportToXml(map);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			exporter.exportToXML(os, true);
			String xml = os.toString("UTF-8");
			System.out.println(xml);
		}
	}
}

