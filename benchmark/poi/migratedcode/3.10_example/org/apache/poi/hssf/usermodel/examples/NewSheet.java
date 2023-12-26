package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.util.WorkbookUtil;


public class NewSheet {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet1 = wb.createSheet("new sheet");
		HSSFSheet sheet2 = wb.createSheet();
		final String name = "second sheet";
		wb.setSheetName(1, WorkbookUtil.createSafeSheetName(name));
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
	}
}

