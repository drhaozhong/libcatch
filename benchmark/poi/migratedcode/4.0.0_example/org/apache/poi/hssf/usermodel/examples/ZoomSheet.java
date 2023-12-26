package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;


public class ZoomSheet {
	public static void main(String[] args) throws IOException {
		try (HSSFWorkbook wb = new HSSFWorkbook()) {
			HSSFSheet sheet1 = wb.createSheet("new sheet");
			sheet1.setZoom(75);
			try (FileOutputStream fileOut = new FileOutputStream("workbook.xls")) {
				wb.write(fileOut);
			}
		}
	}
}

