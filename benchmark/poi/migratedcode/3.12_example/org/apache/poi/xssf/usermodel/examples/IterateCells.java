package org.apache.poi.xssf.usermodel.examples;


import java.io.FileInputStream;
import java.io.PrintStream;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class IterateCells {
	public static void main(String[] args) throws Exception {
		Workbook wb = new XSSFWorkbook(new FileInputStream(args[0]));
		for (int i = 0; i < (wb.getNumberOfSheets()); i++) {
			Sheet sheet = wb.getSheetAt(i);
			System.out.println(wb.getSheetName(i));
			for (Row row : sheet) {
				System.out.println(("rownum: " + (row.getRowNum())));
				for (Cell cell : row) {
					System.out.println(cell.toString());
				}
			}
		}
	}
}

