package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CellType;


public class NewLinesInCells {
	public static void main(String[] args) throws IOException {
		try (HSSFWorkbook wb = new HSSFWorkbook()) {
			HSSFSheet s = wb.createSheet();
			HSSFFont f2 = wb.createFont();
			HSSFCellStyle cs = wb.createCellStyle();
			cs.setFont(f2);
			cs.setWrapText(true);
			HSSFRow r = s.createRow(2);
			r.setHeight(((short) (841)));
			HSSFCell c = r.createCell(2);
			c.setCellType(CellType.STRING);
			c.setCellValue("Use \n with word wrap on to create a new line");
			c.setCellStyle(cs);
			s.setColumnWidth(2, ((int) ((50 * 8) / (((double) (1)) / 20))));
			try (FileOutputStream fileOut = new FileOutputStream("workbook.xls")) {
				wb.write(fileOut);
			}
		}
	}
}

