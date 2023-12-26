package org.apache.poi.xssf.streaming.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;


public class Outlining {
	public static void main(String[] args) throws IOException {
		Outlining o = new Outlining();
		o.collapseRow();
	}

	private void collapseRow() throws IOException {
		try (SXSSFWorkbook wb2 = new SXSSFWorkbook(100)) {
			SXSSFSheet sheet2 = wb2.createSheet("new sheet");
			int rowCount = 20;
			for (int i = 0; i < rowCount; i++) {
				sheet2.createRow(i);
			}
			sheet2.groupRow(4, 9);
			sheet2.groupRow(11, 19);
			sheet2.setRowGroupCollapsed(4, true);
			try (FileOutputStream fileOut = new FileOutputStream("outlining_collapsed.xlsx")) {
				wb2.write(fileOut);
			} finally {
				wb2.dispose();
			}
		}
	}
}

