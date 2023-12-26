package org.apache.poi.xssf.streaming.examples;


import java.io.FileOutputStream;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;


public class Outlining {
	public static void main(String[] args) throws Exception {
		Outlining o = new Outlining();
		o.collapseRow();
	}

	private void collapseRow() throws Exception {
		SXSSFWorkbook wb2 = new SXSSFWorkbook(100);
		SXSSFSheet sheet2 = ((SXSSFSheet) (wb2.createSheet("new sheet")));
		int rowCount = 20;
		for (int i = 0; i < rowCount; i++) {
			sheet2.createRow(i);
		}
		sheet2.groupRow(4, 9);
		sheet2.groupRow(11, 19);
		sheet2.setRowGroupCollapsed(4, true);
		FileOutputStream fileOut = new FileOutputStream("outlining_collapsed.xlsx");
		wb2.write(fileOut);
		fileOut.close();
		wb2.dispose();
	}
}

