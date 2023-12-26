package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class SSPerformanceTest {
	public static void main(String[] args) {
		if ((args.length) != 4)
			SSPerformanceTest.usage("need four command arguments");

		String type = args[0];
		long timeStarted = System.currentTimeMillis();
		Workbook workBook = SSPerformanceTest.createWorkbook(type);
		boolean isHType = workBook instanceof HSSFWorkbook;
		int rows = SSPerformanceTest.parseInt(args[1], "Failed to parse rows value as integer");
		int cols = SSPerformanceTest.parseInt(args[2], "Failed to parse cols value as integer");
		boolean saveFile = (SSPerformanceTest.parseInt(args[3], "Failed to parse saveFile value as integer")) != 0;
		Map<String, CellStyle> styles = SSPerformanceTest.createStyles(workBook);
		Sheet sheet = workBook.createSheet("Main Sheet");
		Cell headerCell = sheet.createRow(0).createCell(0);
		headerCell.setCellValue("Header text is spanned across multiple cells");
		headerCell.setCellStyle(styles.get("header"));
		sheet.addMergedRegion(CellRangeAddress.valueOf("$A$1:$F$1"));
		int sheetNo = 0;
		int rowIndexInSheet = 1;
		double value = 0;
		Calendar calendar = Calendar.getInstance();
		for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
			if (isHType && (sheetNo != (rowIndex / 65536))) {
				sheet = workBook.createSheet(("Spillover from sheet " + (++sheetNo)));
				headerCell.setCellValue("Header text is spanned across multiple cells");
				headerCell.setCellStyle(styles.get("header"));
				sheet.addMergedRegion(CellRangeAddress.valueOf("$A$1:$F$1"));
				rowIndexInSheet = 1;
			}
			Row row = sheet.createRow(rowIndexInSheet);
			for (int colIndex = 0; colIndex < cols; colIndex++) {
				Cell cell = row.createCell(colIndex);
				String address = new CellReference(cell).formatAsString();
				switch (colIndex) {
					case 0 :
						cell.setCellValue((value++));
						break;
					case 1 :
						cell.setCellValue((value++));
						cell.setCellStyle(styles.get("#,##0.00"));
						break;
					case 2 :
						cell.setCellValue((value++));
						cell.setCellStyle(styles.get("$#,##0.00"));
						break;
					case 3 :
						cell.setCellValue(address);
						cell.setCellStyle(styles.get("red-bold"));
						break;
					case 4 :
						cell.setCellValue(((rowIndex % 2) == 0));
						break;
					case 5 :
						cell.setCellValue(calendar);
						cell.setCellStyle(styles.get("m/d/yyyy"));
						calendar.roll(Calendar.DAY_OF_YEAR, (-1));
						break;
					case 6 :
					default :
						cell.setCellValue((value++));
						break;
				}
			}
			rowIndexInSheet++;
		}
		if (saveFile) {
			String fileName = (((((type + "_") + rows) + "_") + cols) + ".") + (SSPerformanceTest.getFileSuffix(args[0]));
			try {
				FileOutputStream out = new FileOutputStream(fileName);
				workBook.write(out);
				out.close();
			} catch (IOException ioe) {
				System.err.println(((("Error: failed to write to file \"" + fileName) + "\", reason=") + (ioe.getMessage())));
			}
		}
		long timeFinished = System.currentTimeMillis();
		System.out.println((("Elapsed " + ((timeFinished - timeStarted) / 1000)) + " seconds"));
	}

	static Map<String, CellStyle> createStyles(Workbook wb) {
		Map<String, CellStyle> styles = new HashMap<String, CellStyle>();
		CellStyle style;
		Font headerFont = wb.createFont();
		headerFont.setFontHeightInPoints(((short) (14)));
		headerFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		style.setFont(headerFont);
		style.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		styles.put("header", style);
		Font monthFont = wb.createFont();
		monthFont.setFontHeightInPoints(((short) (12)));
		monthFont.setColor(IndexedColors.RED.getIndex());
		monthFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		style.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setFont(monthFont);
		styles.put("red-bold", style);
		String[] nfmt = new String[]{ "#,##0.00", "$#,##0.00", "m/d/yyyy" };
		for (String fmt : nfmt) {
			style = wb.createCellStyle();
			style.setDataFormat(wb.createDataFormat().getFormat(fmt));
			styles.put(fmt, style);
		}
		return styles;
	}

	static void usage(String message) {
		System.err.println(message);
		System.err.println("usage: java SSPerformanceTest HSSF|XSSF|SXSSF rows cols saveFile (0|1)? ");
		System.exit(1);
	}

	static Workbook createWorkbook(String type) {
		if ("HSSF".equals(type))
			return new HSSFWorkbook();
		else
			if ("XSSF".equals(type))
				return new XSSFWorkbook();
			else
				if ("SXSSF".equals(type))
					return new SXSSFWorkbook();
				else
					SSPerformanceTest.usage((("Unknown type \"" + type) + "\""));



		return null;
	}

	static String getFileSuffix(String type) {
		if ("HSSF".equals(type))
			return "xls";
		else
			if ("XSSF".equals(type))
				return "xlsx";
			else
				if ("SXSSF".equals(type))
					return "xlsx";



		return null;
	}

	static int parseInt(String value, String msg) {
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			SSPerformanceTest.usage(msg);
		}
		return 0;
	}
}

