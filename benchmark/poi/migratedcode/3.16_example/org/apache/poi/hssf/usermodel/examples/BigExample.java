package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.FillPatternType;

import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.RED;
import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.WHITE;


public class BigExample {
	public static void main(String[] args) throws IOException {
		int rownum;
		FileOutputStream out = new FileOutputStream("workbook.xls");
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet s = wb.createSheet();
		HSSFRow r = null;
		HSSFCell c = null;
		HSSFCellStyle cs = wb.createCellStyle();
		HSSFCellStyle cs2 = wb.createCellStyle();
		HSSFCellStyle cs3 = wb.createCellStyle();
		HSSFFont f = wb.createFont();
		HSSFFont f2 = wb.createFont();
		f.setFontHeightInPoints(((short) (12)));
		f.setColor(RED.getIndex());
		f.setBold(true);
		f2.setFontHeightInPoints(((short) (10)));
		f2.setColor(WHITE.getIndex());
		f2.setBold(true);
		cs.setFont(f);
		cs.setDataFormat(HSSFDataFormat.getBuiltinFormat("($#,##0_);[Red]($#,##0)"));
		cs2.setBorderBottom(BorderStyle.THIN);
		cs2.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		cs2.setFillForegroundColor(RED.getIndex());
		cs2.setFont(f2);
		wb.setSheetName(0, "HSSF Test");
		for (rownum = 0; rownum < 300; rownum++) {
			r = s.createRow(rownum);
			if ((rownum % 2) == 0) {
				r.setHeight(((short) (585)));
			}
			for (int cellnum = 0; cellnum < 50; cellnum += 2) {
				c = r.createCell(cellnum);
				c.setCellValue((((rownum * 10000) + cellnum) + ((((double) (rownum)) / 1000) + (((double) (cellnum)) / 10000))));
				if ((rownum % 2) == 0) {
					c.setCellStyle(cs);
				}
				c = r.createCell((cellnum + 1));
				c.setCellValue("TEST");
				s.setColumnWidth((cellnum + 1), ((int) ((50 * 8) / (((double) (1)) / 20))));
				if ((rownum % 2) == 0) {
					c.setCellStyle(cs2);
				}
			}
		}
		rownum++;
		rownum++;
		r = s.createRow(rownum);
		cs3.setBorderBottom(BorderStyle.THICK);
		for (int cellnum = 0; cellnum < 50; cellnum++) {
			c = r.createCell(cellnum);
			c.setCellStyle(cs3);
		}
		wb.createSheet();
		wb.setSheetName(1, "DeletedSheet");
		wb.removeSheetAt(1);
		wb.write(out);
		out.close();
		wb.close();
	}
}

