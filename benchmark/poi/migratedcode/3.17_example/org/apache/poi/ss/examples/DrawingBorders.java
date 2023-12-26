package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.BorderExtent;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.PropertyTemplate;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class DrawingBorders {
	public static void main(String[] args) throws IOException {
		Workbook wb;
		if (((args.length) > 0) && (args[0].equals("-xls"))) {
			wb = new HSSFWorkbook();
		}else {
			wb = new XSSFWorkbook();
		}
		Sheet sh1 = wb.createSheet("Sheet1");
		Row r = sh1.createRow(0);
		Cell c = r.createCell(1);
		c.setCellValue("All Borders Medium Width");
		r = sh1.createRow(4);
		c = r.createCell(1);
		c.setCellValue("Medium Outside / Thin Inside Borders");
		r = sh1.createRow(8);
		c = r.createCell(1);
		c.setCellValue("Colored Borders");
		PropertyTemplate pt = new PropertyTemplate();
		pt.drawBorders(new CellRangeAddress(1, 3, 1, 3), BorderStyle.MEDIUM, BorderExtent.ALL);
		pt.drawBorders(new CellRangeAddress(5, 7, 1, 3), BorderStyle.MEDIUM, BorderExtent.OUTSIDE);
		pt.drawBorders(new CellRangeAddress(5, 7, 1, 3), BorderStyle.THIN, BorderExtent.INSIDE);
		pt.drawBorders(new CellRangeAddress(9, 11, 1, 3), BorderStyle.MEDIUM, IndexedColors.RED.getIndex(), BorderExtent.OUTSIDE);
		pt.drawBorders(new CellRangeAddress(9, 11, 1, 3), BorderStyle.MEDIUM, IndexedColors.BLUE.getIndex(), BorderExtent.INSIDE_VERTICAL);
		pt.drawBorders(new CellRangeAddress(9, 11, 1, 3), BorderStyle.MEDIUM, IndexedColors.GREEN.getIndex(), BorderExtent.INSIDE_HORIZONTAL);
		pt.drawBorders(new CellRangeAddress(10, 10, 2, 2), BorderStyle.NONE, BorderExtent.ALL);
		pt.applyBorders(sh1);
		Sheet sh2 = wb.createSheet("Sheet2");
		pt.applyBorders(sh2);
		String file = "db-poi.xls";
		if (wb instanceof XSSFWorkbook)
			file += "x";

		FileOutputStream out = new FileOutputStream(file);
		wb.write(out);
		out.close();
		wb.close();
		System.out.println(("Generated: " + file));
	}
}

