package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class Outlining {
	public static void main(String[] args) throws Exception {
		Outlining o = new Outlining();
		o.groupRowColumn();
		o.collapseExpandRowColumn();
	}

	private void groupRowColumn() throws Exception {
		Workbook wb = new XSSFWorkbook();
		Sheet sheet1 = wb.createSheet("new sheet");
		sheet1.groupRow(5, 14);
		sheet1.groupRow(7, 14);
		sheet1.groupRow(16, 19);
		sheet1.groupColumn(((short) (4)), ((short) (7)));
		sheet1.groupColumn(((short) (9)), ((short) (12)));
		sheet1.groupColumn(((short) (10)), ((short) (11)));
		FileOutputStream fileOut = new FileOutputStream("outlining.xlsx");
		wb.write(fileOut);
		fileOut.close();
	}

	private void collapseExpandRowColumn() throws Exception {
		Workbook wb2 = new XSSFWorkbook();
		Sheet sheet2 = wb2.createSheet("new sheet");
		sheet2.groupRow(5, 14);
		sheet2.groupRow(7, 14);
		sheet2.groupRow(16, 19);
		sheet2.groupColumn(((short) (4)), ((short) (7)));
		sheet2.groupColumn(((short) (9)), ((short) (12)));
		sheet2.groupColumn(((short) (10)), ((short) (11)));
		sheet2.setRowGroupCollapsed(7, true);
		sheet2.setColumnGroupCollapsed(((short) (4)), true);
		sheet2.setColumnGroupCollapsed(((short) (4)), false);
		FileOutputStream fileOut = new FileOutputStream("outlining_collapsed.xlsx");
		wb2.write(fileOut);
		fileOut.close();
	}
}

