package org.apache.poi.xssf.usermodel.examples;


import java.awt.Color;
import java.io.FileOutputStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.PrintOrientation;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFPrintSetup;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class CalendarDemo {
	private static final String[] days = new String[]{ "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" };

	private static final String[] months = new String[]{ "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December" };

	public static void main(String[] args) throws Exception {
		Calendar calendar = Calendar.getInstance();
		if ((args.length) > 0)
			calendar.set(Calendar.YEAR, Integer.parseInt(args[0]));

		int year = calendar.get(Calendar.YEAR);
		XSSFWorkbook wb = new XSSFWorkbook();
		Map<String, XSSFCellStyle> styles = CalendarDemo.createStyles(wb);
		for (int month = 0; month < 12; month++) {
			calendar.set(Calendar.MONTH, month);
			calendar.set(Calendar.DAY_OF_MONTH, 1);
			XSSFSheet sheet = wb.createSheet(CalendarDemo.months[month]);
			sheet.setDisplayGridlines(false);
			sheet.setPrintGridlines(false);
			XSSFPrintSetup printSetup = sheet.getPrintSetup();
			printSetup.setOrientation(PrintOrientation.LANDSCAPE);
			sheet.setFitToPage(true);
			sheet.setHorizontallyCenter(true);
			XSSFRow headerRow = sheet.createRow(0);
			headerRow.setHeightInPoints(80);
			XSSFCell titleCell = headerRow.createCell(0);
			titleCell.setCellValue((((CalendarDemo.months[month]) + " ") + year));
			titleCell.setCellStyle(styles.get("title"));
			sheet.addMergedRegion(CellRangeAddress.valueOf("$A$1:$N$1"));
			XSSFRow monthRow = sheet.createRow(1);
			for (int i = 0; i < (CalendarDemo.days.length); i++) {
				sheet.setColumnWidth((i * 2), (5 * 256));
				sheet.setColumnWidth(((i * 2) + 1), (13 * 256));
				sheet.addMergedRegion(new CellRangeAddress(1, 1, (i * 2), ((i * 2) + 1)));
				XSSFCell monthCell = monthRow.createCell((i * 2));
				monthCell.setCellValue(CalendarDemo.days[i]);
				monthCell.setCellStyle(styles.get("month"));
			}
			int cnt = 1;
			int day = 1;
			int rownum = 2;
			for (int j = 0; j < 6; j++) {
				XSSFRow row = sheet.createRow((rownum++));
				row.setHeightInPoints(100);
				for (int i = 0; i < (CalendarDemo.days.length); i++) {
					XSSFCell dayCell_1 = row.createCell((i * 2));
					XSSFCell dayCell_2 = row.createCell(((i * 2) + 1));
					int day_of_week = calendar.get(Calendar.DAY_OF_WEEK);
					if ((cnt >= day_of_week) && ((calendar.get(Calendar.MONTH)) == month)) {
						dayCell_1.setCellValue(day);
						calendar.set(Calendar.DAY_OF_MONTH, (++day));
						if ((i == 0) || (i == ((CalendarDemo.days.length) - 1))) {
							dayCell_1.setCellStyle(styles.get("weekend_left"));
							dayCell_2.setCellStyle(styles.get("weekend_right"));
						}else {
							dayCell_1.setCellStyle(styles.get("workday_left"));
							dayCell_2.setCellStyle(styles.get("workday_right"));
						}
					}else {
						dayCell_1.setCellStyle(styles.get("grey_left"));
						dayCell_2.setCellStyle(styles.get("grey_right"));
					}
					cnt++;
				}
				if ((calendar.get(Calendar.MONTH)) > month)
					break;

			}
		}
		FileOutputStream out = new FileOutputStream((("calendar-" + year) + ".xlsx"));
		wb.write(out);
		out.close();
		wb.close();
	}

	private static Map<String, XSSFCellStyle> createStyles(XSSFWorkbook wb) {
		Map<String, XSSFCellStyle> styles = new HashMap<String, XSSFCellStyle>();
		XSSFCellStyle style;
		XSSFFont titleFont = wb.createFont();
		titleFont.setFontHeightInPoints(((short) (48)));
		style = wb.createCellStyle();
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setVerticalAlignment(VerticalAlignment.CENTER);
		style.setFont(titleFont);
		styles.put("title", style);
		XSSFFont monthFont = wb.createFont();
		monthFont.setFontHeightInPoints(((short) (12)));
		monthFont.setColor(new XSSFColor(new Color(255, 255, 255)));
		monthFont.setBold(true);
		style = wb.createCellStyle();
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setVerticalAlignment(VerticalAlignment.CENTER);
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setFont(monthFont);
		styles.put("month", style);
		XSSFFont dayFont = wb.createFont();
		dayFont.setFontHeightInPoints(((short) (14)));
		dayFont.setBold(true);
		style = wb.createCellStyle();
		style.setAlignment(HorizontalAlignment.LEFT);
		style.setVerticalAlignment(VerticalAlignment.TOP);
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setBorderLeft(BorderStyle.THIN);
		style.setLeftBorderColor(new XSSFColor(new Color(39, 51, 89)));
		style.setBorderBottom(BorderStyle.THIN);
		style.setFont(dayFont);
		styles.put("weekend_left", style);
		style = wb.createCellStyle();
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setVerticalAlignment(VerticalAlignment.TOP);
		style.setFillForegroundColor(new XSSFColor(new Color(228, 232, 243)));
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setBorderRight(BorderStyle.THIN);
		style.setBorderBottom(BorderStyle.THIN);
		styles.put("weekend_right", style);
		style = wb.createCellStyle();
		style.setAlignment(HorizontalAlignment.LEFT);
		style.setVerticalAlignment(VerticalAlignment.TOP);
		style.setBorderLeft(BorderStyle.THIN);
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setLeftBorderColor(new XSSFColor(new Color(39, 51, 89)));
		style.setBorderBottom(BorderStyle.THIN);
		style.setFont(dayFont);
		styles.put("workday_left", style);
		style = wb.createCellStyle();
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setVerticalAlignment(VerticalAlignment.TOP);
		style.setFillForegroundColor(new XSSFColor());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setBorderRight(BorderStyle.THIN);
		style.setBorderBottom(BorderStyle.THIN);
		style.setBottomBorderColor(new XSSFColor(new Color(39, 51, 89)));
		styles.put("workday_right", style);
		style = wb.createCellStyle();
		style.setBorderLeft(BorderStyle.THIN);
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setBorderBottom(BorderStyle.THIN);
		style.setBottomBorderColor(new XSSFColor(new Color(39, 51, 89)));
		styles.put("grey_left", style);
		style = wb.createCellStyle();
		style.setFillForegroundColor(new XSSFColor(new Color(234, 234, 234)));
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setBorderRight(BorderStyle.THIN);
		style.setBorderBottom(BorderStyle.THIN);
		style.setBottomBorderColor(new XSSFColor(new Color(39, 51, 89)));
		styles.put("grey_right", style);
		return styles;
	}
}

