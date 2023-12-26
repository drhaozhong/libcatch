package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.PrintSetup;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class CalendarDemo {
	private static final String[] days = new String[]{ "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" };

	private static final String[] months = new String[]{ "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December" };

	public static void main(String[] args) throws Exception {
		Calendar calendar = Calendar.getInstance();
		boolean xlsx = true;
		for (int i = 0; i < (args.length); i++) {
			if ((args[i].charAt(0)) == '-') {
				xlsx = args[i].equals("-xlsx");
			}else {
				calendar.set(Calendar.YEAR, Integer.parseInt(args[i]));
			}
		}
		int year = calendar.get(Calendar.YEAR);
		Workbook wb = (xlsx) ? new XSSFWorkbook() : new HSSFWorkbook();
		Map<String, CellStyle> styles = CalendarDemo.createStyles(wb);
		for (int month = 0; month < 12; month++) {
			calendar.set(Calendar.MONTH, month);
			calendar.set(Calendar.DAY_OF_MONTH, 1);
			Sheet sheet = wb.createSheet(CalendarDemo.months[month]);
			sheet.setDisplayGridlines(false);
			sheet.setPrintGridlines(false);
			sheet.setFitToPage(true);
			sheet.setHorizontallyCenter(true);
			PrintSetup printSetup = sheet.getPrintSetup();
			printSetup.setLandscape(true);
			sheet.setAutobreaks(true);
			printSetup.setFitHeight(((short) (1)));
			printSetup.setFitWidth(((short) (1)));
			Row headerRow = sheet.createRow(0);
			headerRow.setHeightInPoints(80);
			Cell titleCell = headerRow.createCell(0);
			titleCell.setCellValue((((CalendarDemo.months[month]) + " ") + year));
			titleCell.setCellStyle(styles.get("title"));
			sheet.addMergedRegion(CellRangeAddress.valueOf("$A$1:$N$1"));
			Row monthRow = sheet.createRow(1);
			for (int i = 0; i < (CalendarDemo.days.length); i++) {
				sheet.setColumnWidth((i * 2), (5 * 256));
				sheet.setColumnWidth(((i * 2) + 1), (13 * 256));
				sheet.addMergedRegion(new CellRangeAddress(1, 1, (i * 2), ((i * 2) + 1)));
				Cell monthCell = monthRow.createCell((i * 2));
				monthCell.setCellValue(CalendarDemo.days[i]);
				monthCell.setCellStyle(styles.get("month"));
			}
			int cnt = 1;
			int day = 1;
			int rownum = 2;
			for (int j = 0; j < 6; j++) {
				Row row = sheet.createRow((rownum++));
				row.setHeightInPoints(100);
				for (int i = 0; i < (CalendarDemo.days.length); i++) {
					Cell dayCell_1 = row.createCell((i * 2));
					Cell dayCell_2 = row.createCell(((i * 2) + 1));
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
		String file = "calendar.xls";
		if (wb instanceof XSSFWorkbook)
			file += "x";

		FileOutputStream out = new FileOutputStream(file);
		wb.write(out);
		out.close();
	}

	private static Map<String, CellStyle> createStyles(Workbook wb) {
		Map<String, CellStyle> styles = new HashMap<String, CellStyle>();
		short borderColor = IndexedColors.GREY_50_PERCENT.getIndex();
		CellStyle style;
		Font titleFont = wb.createFont();
		titleFont.setFontHeightInPoints(((short) (48)));
		titleFont.setColor(IndexedColors.DARK_BLUE.getIndex());
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		style.setFont(titleFont);
		styles.put("title", style);
		Font monthFont = wb.createFont();
		monthFont.setFontHeightInPoints(((short) (12)));
		monthFont.setColor(IndexedColors.WHITE.getIndex());
		monthFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		style.setFillForegroundColor(IndexedColors.DARK_BLUE.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setFont(monthFont);
		styles.put("month", style);
		Font dayFont = wb.createFont();
		dayFont.setFontHeightInPoints(((short) (14)));
		dayFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_LEFT);
		style.setVerticalAlignment(CellStyle.VERTICAL_TOP);
		style.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setBorderLeft(CellStyle.BORDER_THIN);
		style.setLeftBorderColor(borderColor);
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setBottomBorderColor(borderColor);
		style.setFont(dayFont);
		styles.put("weekend_left", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setVerticalAlignment(CellStyle.VERTICAL_TOP);
		style.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setBorderRight(CellStyle.BORDER_THIN);
		style.setRightBorderColor(borderColor);
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setBottomBorderColor(borderColor);
		styles.put("weekend_right", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_LEFT);
		style.setVerticalAlignment(CellStyle.VERTICAL_TOP);
		style.setBorderLeft(CellStyle.BORDER_THIN);
		style.setFillForegroundColor(IndexedColors.WHITE.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setLeftBorderColor(borderColor);
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setBottomBorderColor(borderColor);
		style.setFont(dayFont);
		styles.put("workday_left", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setVerticalAlignment(CellStyle.VERTICAL_TOP);
		style.setFillForegroundColor(IndexedColors.WHITE.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setBorderRight(CellStyle.BORDER_THIN);
		style.setRightBorderColor(borderColor);
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setBottomBorderColor(borderColor);
		styles.put("workday_right", style);
		style = wb.createCellStyle();
		style.setBorderLeft(CellStyle.BORDER_THIN);
		style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setBottomBorderColor(borderColor);
		styles.put("grey_left", style);
		style = wb.createCellStyle();
		style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setBorderRight(CellStyle.BORDER_THIN);
		style.setRightBorderColor(borderColor);
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setBottomBorderColor(borderColor);
		styles.put("grey_right", style);
		return styles;
	}
}

