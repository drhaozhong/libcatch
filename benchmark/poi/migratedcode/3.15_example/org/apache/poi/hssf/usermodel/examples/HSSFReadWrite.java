package org.apache.poi.hssf.usermodel.examples;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Locale;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.util.CellRangeAddress;


public final class HSSFReadWrite {
	private static HSSFWorkbook readFile(String filename) throws IOException {
		FileInputStream fis = new FileInputStream(filename);
		try {
			return new HSSFWorkbook(fis);
		} finally {
			fis.close();
		}
	}

	private static void testCreateSampleSheet(String outputFilename) throws IOException {
		int rownum;
		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet s = wb.createSheet();
		HSSFCellStyle cs = wb.createCellStyle();
		HSSFCellStyle cs2 = wb.createCellStyle();
		HSSFCellStyle cs3 = wb.createCellStyle();
		HSSFFont f = wb.createFont();
		HSSFFont f2 = wb.createFont();
		f.setFontHeightInPoints(((short) (12)));
		f.setColor(((short) (10)));
		f.setBold(true);
		f2.setFontHeightInPoints(((short) (10)));
		f2.setColor(((short) (15)));
		f2.setBold(true);
		cs.setFont(f);
		cs.setDataFormat(HSSFDataFormat.getBuiltinFormat("($#,##0_);[Red]($#,##0)"));
		cs2.setBorderBottom(BorderStyle.THIN);
		cs2.setFillPattern(((short) (1)));
		cs2.setFillForegroundColor(((short) (10)));
		cs2.setFont(f2);
		wb.setSheetName(0, "HSSF Test");
		for (rownum = 0; rownum < 300; rownum++) {
			HSSFRow r = s.createRow(rownum);
			if ((rownum % 2) == 0) {
				r.setHeight(((short) (585)));
			}
			for (int cellnum = 0; cellnum < 50; cellnum += 2) {
				HSSFCell c = r.createCell(cellnum);
				c.setCellValue((((rownum * 10000) + cellnum) + ((((double) (rownum)) / 1000) + (((double) (cellnum)) / 10000))));
				if ((rownum % 2) == 0) {
					c.setCellStyle(cs);
				}
				c = r.createCell((cellnum + 1));
				c.setCellValue(new HSSFRichTextString("TEST"));
				s.setColumnWidth((cellnum + 1), ((int) ((50 * 8) / 0.05)));
				if ((rownum % 2) == 0) {
					c.setCellStyle(cs2);
				}
			}
		}
		rownum++;
		rownum++;
		HSSFRow r = s.createRow(rownum);
		cs3.setBorderBottom(BorderStyle.THICK);
		for (int cellnum = 0; cellnum < 50; cellnum++) {
			HSSFCell c = r.createCell(cellnum);
			c.setCellStyle(cs3);
		}
		s.addMergedRegion(new CellRangeAddress(0, 3, 0, 3));
		s.addMergedRegion(new CellRangeAddress(100, 110, 100, 110));
		wb.createSheet();
		wb.setSheetName(1, "DeletedSheet");
		wb.removeSheetAt(1);
		FileOutputStream out = new FileOutputStream(outputFilename);
		try {
			wb.write(out);
		} finally {
			out.close();
		}
		wb.close();
	}

	public static void main(String[] args) {
		if ((args.length) < 1) {
			System.err.println("At least one argument expected");
			return;
		}
		String fileName = args[0];
		try {
			if ((args.length) < 2) {
				HSSFWorkbook wb = HSSFReadWrite.readFile(fileName);
				System.out.println("Data dump:\n");
				for (int k = 0; k < (wb.getNumberOfSheets()); k++) {
					HSSFSheet sheet = wb.getSheetAt(k);
					int rows = sheet.getPhysicalNumberOfRows();
					System.out.println((((((("Sheet " + k) + " \"") + (wb.getSheetName(k))) + "\" has ") + rows) + " row(s)."));
					for (int r = 0; r < rows; r++) {
						HSSFRow row = sheet.getRow(r);
						if (row == null) {
							continue;
						}
						int cells = row.getPhysicalNumberOfCells();
						System.out.println((((("\nROW " + (row.getRowNum())) + " has ") + cells) + " cell(s)."));
						for (int c = 0; c < cells; c++) {
							HSSFCell cell = row.getCell(c);
							String value = null;
							switch (cell.getCellTypeEnum()) {
								case FORMULA :
									value = "FORMULA value=" + (cell.getCellFormula());
									break;
								case NUMERIC :
									value = "NUMERIC value=" + (cell.getNumericCellValue());
									break;
								case STRING :
									value = "STRING value=" + (cell.getStringCellValue());
									break;
								default :
							}
							System.out.println(((("CELL col=" + (cell.getColumnIndex())) + " VALUE=") + value));
						}
					}
				}
				wb.close();
			}else
				if ((args.length) == 2) {
					if (args[1].toLowerCase(Locale.ROOT).equals("write")) {
						System.out.println("Write mode");
						long time = System.currentTimeMillis();
						HSSFReadWrite.testCreateSampleSheet(fileName);
						System.out.println((("" + ((System.currentTimeMillis()) - time)) + " ms generation time"));
					}else {
						System.out.println("readwrite test");
						HSSFWorkbook wb = HSSFReadWrite.readFile(fileName);
						FileOutputStream stream = new FileOutputStream(args[1]);
						wb.write(stream);
						stream.close();
						wb.close();
					}
				}else
					if (((args.length) == 3) && (args[2].toLowerCase(Locale.ROOT).equals("modify1"))) {
						HSSFWorkbook wb = HSSFReadWrite.readFile(fileName);
						FileOutputStream stream = new FileOutputStream(args[1]);
						HSSFSheet sheet = wb.getSheetAt(0);
						for (int k = 0; k < 25; k++) {
							HSSFRow row = sheet.getRow(k);
							sheet.removeRow(row);
						}
						for (int k = 74; k < 100; k++) {
							HSSFRow row = sheet.getRow(k);
							sheet.removeRow(row);
						}
						HSSFRow row = sheet.getRow(39);
						HSSFCell cell = row.getCell(3);
						cell.setCellValue("MODIFIED CELL!!!!!");
						wb.write(stream);
						stream.close();
						wb.close();
					}


		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

