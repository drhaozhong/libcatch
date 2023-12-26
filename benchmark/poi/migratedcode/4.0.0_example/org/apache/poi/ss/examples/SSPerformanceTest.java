package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ooxml.POIXMLTypeLoader;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.xmlbeans.XmlOptions;


public class SSPerformanceTest {
	public static void main(String[] args) throws IOException {
		if ((args.length) < 4) {
			SSPerformanceTest.usage("need at least four command arguments");
		}
		String type = args[0];
		int rows = SSPerformanceTest.parseInt(args[1], "Failed to parse rows value as integer");
		int cols = SSPerformanceTest.parseInt(args[2], "Failed to parse cols value as integer");
		boolean saveFile = (SSPerformanceTest.parseInt(args[3], "Failed to parse saveFile value as integer")) != 0;
		boolean warmup = false;
		for (int arg = 4; arg < (args.length); arg++) {
			if (args[arg].equals("--unsynchronized-xmlbeans")) {
				POIXMLTypeLoader.DEFAULT_XML_OPTIONS.setUnsynchronized();
			}
			if (args[arg].equals("--with-warmup-run")) {
				warmup = true;
			}
		}
		if (warmup) {
			System.out.println("Performing a warmup run first");
			SSPerformanceTest.runWithArgs(type, rows, cols, saveFile);
		}
		long timeStarted = System.currentTimeMillis();
		SSPerformanceTest.runWithArgs(type, rows, cols, saveFile);
		long timeFinished = System.currentTimeMillis();
		System.out.printf("Elapsed %.2f seconds for arguments %s\n", ((((double) (timeFinished)) - timeStarted) / 1000), Arrays.toString(args));
	}

	private static void runWithArgs(String type, int rows, int cols, boolean saveFile) throws IOException {
		try (Workbook workBook = SSPerformanceTest.createWorkbook(type)) {
			boolean isHType = workBook instanceof HSSFWorkbook;
			SSPerformanceTest.addContent(workBook, isHType, rows, cols);
			if (saveFile) {
				String fileName = (((((type + "_") + rows) + "_") + cols) + ".") + (SSPerformanceTest.getFileSuffix(type));
				SSPerformanceTest.saveFile(workBook, fileName);
			}
		}
	}

	private static void addContent(Workbook workBook, boolean isHType, int rows, int cols) {
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
				value = SSPerformanceTest.populateCell(styles, value, calendar, rowIndex, row, colIndex);
			}
			rowIndexInSheet++;
		}
	}

	private static double populateCell(Map<String, CellStyle> styles, double value, Calendar calendar, int rowIndex, Row row, int colIndex) {
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
		return value;
	}

	private static void saveFile(Workbook workBook, String fileName) {
		try {
			FileOutputStream out = new FileOutputStream(fileName);
			workBook.write(out);
			out.close();
		} catch (IOException ioe) {
			System.err.println(((("Error: failed to write to file \"" + fileName) + "\", reason=") + (ioe.getMessage())));
		}
	}

	static Map<String, CellStyle> createStyles(Workbook wb) {
		Map<String, CellStyle> styles = new HashMap<>();
		CellStyle style;
		Font headerFont = wb.createFont();
		headerFont.setFontHeightInPoints(((short) (14)));
		headerFont.setBold(true);
		style = wb.createCellStyle();
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setVerticalAlignment(VerticalAlignment.CENTER);
		style.setFont(headerFont);
		style.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		styles.put("header", style);
		Font monthFont = wb.createFont();
		monthFont.setFontHeightInPoints(((short) (12)));
		monthFont.setColor(IndexedColors.RED.getIndex());
		monthFont.setBold(true);
		style = wb.createCellStyle();
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setVerticalAlignment(VerticalAlignment.CENTER);
		style.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
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
		System.err.println("usage: java SSPerformanceTest HSSF|XSSF|SXSSF rows cols saveFile (0|1)? [--unsynchronized-xmlbeans] [--with-warmup-run]");
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



		SSPerformanceTest.usage((("Unknown type \"" + type) + "\""));
		throw new IllegalArgumentException("Should not reach this point");
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

