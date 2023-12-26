package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class SelectedSheet {
	public static void main(String[] args) throws Exception {
		Workbook wb = new XSSFWorkbook();
		Sheet sheet = wb.createSheet("row sheet");
		Sheet sheet2 = wb.createSheet("another sheet");
		Sheet sheet3 = wb.createSheet(" sheet 3 ");
		sheet3.setSelected(true);
		wb.setActiveSheet(2);
		FileOutputStream fileOut = new FileOutputStream("selectedSheet.xlsx");
		wb.write(fileOut);
		fileOut.close();
	}
}

