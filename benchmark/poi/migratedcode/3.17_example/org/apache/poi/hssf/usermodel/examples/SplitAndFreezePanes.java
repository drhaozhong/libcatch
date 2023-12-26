package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;


public class SplitAndFreezePanes {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet1 = wb.createSheet("new sheet");
		HSSFSheet sheet2 = wb.createSheet("second sheet");
		HSSFSheet sheet3 = wb.createSheet("third sheet");
		HSSFSheet sheet4 = wb.createSheet("fourth sheet");
		sheet1.createFreezePane(0, 1, 0, 1);
		sheet2.createFreezePane(1, 0, 1, 0);
		sheet3.createFreezePane(2, 2);
		sheet4.createSplitPane(2000, 2000, 0, 0, Sheet.PANE_LOWER_LEFT);
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
		wb.close();
	}
}

