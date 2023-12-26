package org.apache.poi.hssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;


public class NewLinesInCells {
	public static void main(String[] args) throws IOException {
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet s = wb.createSheet();
		HSSFRow r = null;
		HSSFCell c = null;
		HSSFCellStyle cs = wb.createCellStyle();
		HSSFFont f = wb.createFont();
		HSSFFont f2 = wb.createFont();
		cs = wb.createCellStyle();
		cs.setFont(f2);
		cs.setWrapText(true);
		r = s.createRow(((short) (2)));
		r.setHeight(((short) (841)));
		c = r.createCell(((short) (2)));
		c.setCellType(HSSFCell.CELL_TYPE_STRING);
		c.setCellValue("Use \n with word wrap on to create a new line");
		c.setCellStyle(cs);
		s.setColumnWidth(((short) (2)), ((short) ((50 * 8) / (((double) (1)) / 20))));
		FileOutputStream fileOut = new FileOutputStream("workbook.xls");
		wb.write(fileOut);
		fileOut.close();
	}
}

