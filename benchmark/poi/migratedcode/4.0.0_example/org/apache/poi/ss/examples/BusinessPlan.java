package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.PrintSetup;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class BusinessPlan {
	private static SimpleDateFormat fmt = new SimpleDateFormat("dd-MMM");

	private static final String[] titles = new String[]{ "ID", "Project Name", "Owner", "Days", "Start", "End" };

	private static final String[][] data = new String[][]{ new String[]{ "1.0", "Marketing Research Tactical Plan", "J. Dow", "70", "9-Jul", null, "x", "x", "x", "x", "x", "x", "x", "x", "x", "x", "x" }, null, new String[]{ "1.1", "Scope Definition Phase", "J. Dow", "10", "9-Jul", null, "x", "x", null, null, null, null, null, null, null, null, null }, new String[]{ "1.1.1", "Define research objectives", "J. Dow", "3", "9-Jul", null, "x", null, null, null, null, null, null, null, null, null, null }, new String[]{ "1.1.2", "Define research requirements", "S. Jones", "7", "10-Jul", null, "x", "x", null, null, null, null, null, null, null, null, null }, new String[]{ "1.1.3", "Determine in-house resource or hire vendor", "J. Dow", "2", "15-Jul", null, "x", "x", null, null, null, null, null, null, null, null, null }, null, new String[]{ "1.2", "Vendor Selection Phase", "J. Dow", "19", "19-Jul", null, null, "x", "x", "x", "x", null, null, null, null, null, null }, new String[]{ "1.2.1", "Define vendor selection criteria", "J. Dow", "3", "19-Jul", null, null, "x", null, null, null, null, null, null, null, null, null }, new String[]{ "1.2.2", "Develop vendor selection questionnaire", "S. Jones, T. Wates", "2", "22-Jul", null, null, "x", "x", null, null, null, null, null, null, null, null }, new String[]{ "1.2.3", "Develop Statement of Work", "S. Jones", "4", "26-Jul", null, null, null, "x", "x", null, null, null, null, null, null, null }, new String[]{ "1.2.4", "Evaluate proposal", "J. Dow, S. Jones", "4", "2-Aug", null, null, null, null, "x", "x", null, null, null, null, null, null }, new String[]{ "1.2.5", "Select vendor", "J. Dow", "1", "6-Aug", null, null, null, null, null, "x", null, null, null, null, null, null }, null, new String[]{ "1.3", "Research Phase", "G. Lee", "47", "9-Aug", null, null, null, null, null, "x", "x", "x", "x", "x", "x", "x" }, new String[]{ "1.3.1", "Develop market research information needs questionnaire", "G. Lee", "2", "9-Aug", null, null, null, null, null, "x", null, null, null, null, null, null }, new String[]{ "1.3.2", "Interview marketing group for market research needs", "G. Lee", "2", "11-Aug", null, null, null, null, null, "x", "x", null, null, null, null, null }, new String[]{ "1.3.3", "Document information needs", "G. Lee, S. Jones", "1", "13-Aug", null, null, null, null, null, null, "x", null, null, null, null, null } };

	public static void main(String[] args) throws Exception {
		Workbook wb;
		if (((args.length) > 0) && (args[0].equals("-xls")))
			wb = new HSSFWorkbook();
		else
			wb = new XSSFWorkbook();

		Map<String, CellStyle> styles = BusinessPlan.createStyles(wb);
		Sheet sheet = wb.createSheet("Business Plan");
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
		headerRow.setHeightInPoints(12.75F);
		for (int i = 0; i < (BusinessPlan.titles.length); i++) {
			Cell cell = headerRow.createCell(i);
			cell.setCellValue(BusinessPlan.titles[i]);
			cell.setCellStyle(styles.get("header"));
		}
		Calendar calendar = Calendar.getInstance();
		int year = calendar.get(Calendar.YEAR);
		calendar.setTime(BusinessPlan.fmt.parse("9-Jul"));
		calendar.set(Calendar.YEAR, year);
		for (int i = 0; i < 11; i++) {
			Cell cell = headerRow.createCell(((BusinessPlan.titles.length) + i));
			cell.setCellValue(calendar);
			cell.setCellStyle(styles.get("header_date"));
			calendar.roll(Calendar.WEEK_OF_YEAR, true);
		}
		sheet.createFreezePane(0, 1);
		Row row;
		Cell cell;
		int rownum = 1;
		for (int i = 0; i < (BusinessPlan.data.length); i++ , rownum++) {
			row = sheet.createRow(rownum);
			if ((BusinessPlan.data[i]) == null)
				continue;

			for (int j = 0; j < (BusinessPlan.data[i].length); j++) {
				cell = row.createCell(j);
				String styleName;
				boolean isHeader = (i == 0) || ((BusinessPlan.data[(i - 1)]) == null);
				switch (j) {
					case 0 :
						if (isHeader) {
							styleName = "cell_b";
							cell.setCellValue(Double.parseDouble(BusinessPlan.data[i][j]));
						}else {
							styleName = "cell_normal";
							cell.setCellValue(BusinessPlan.data[i][j]);
						}
						break;
					case 1 :
						if (isHeader) {
							styleName = (i == 0) ? "cell_h" : "cell_bb";
						}else {
							styleName = "cell_indented";
						}
						cell.setCellValue(BusinessPlan.data[i][j]);
						break;
					case 2 :
						styleName = (isHeader) ? "cell_b" : "cell_normal";
						cell.setCellValue(BusinessPlan.data[i][j]);
						break;
					case 3 :
						styleName = (isHeader) ? "cell_b_centered" : "cell_normal_centered";
						cell.setCellValue(Integer.parseInt(BusinessPlan.data[i][j]));
						break;
					case 4 :
						{
							calendar.setTime(BusinessPlan.fmt.parse(BusinessPlan.data[i][j]));
							calendar.set(Calendar.YEAR, year);
							cell.setCellValue(calendar);
							styleName = (isHeader) ? "cell_b_date" : "cell_normal_date";
							break;
						}
					case 5 :
						{
							int r = rownum + 1;
							String fmla = ((((((("IF(AND(D" + r) + ",E") + r) + "),E") + r) + "+D") + r) + ",\"\")";
							cell.setCellFormula(fmla);
							styleName = (isHeader) ? "cell_bg" : "cell_g";
							break;
						}
					default :
						styleName = ((BusinessPlan.data[i][j]) != null) ? "cell_blue" : "cell_normal";
				}
				cell.setCellStyle(styles.get(styleName));
			}
		}
		sheet.groupRow(4, 6);
		sheet.groupRow(9, 13);
		sheet.groupRow(16, 18);
		sheet.setColumnWidth(0, (256 * 6));
		sheet.setColumnWidth(1, (256 * 33));
		sheet.setColumnWidth(2, (256 * 20));
		sheet.setZoom(75);
		String file = "businessplan.xls";
		if (wb instanceof XSSFWorkbook)
			file += "x";

		FileOutputStream out = new FileOutputStream(file);
		wb.write(out);
		out.close();
		wb.close();
	}

	private static Map<String, CellStyle> createStyles(Workbook wb) {
		Map<String, CellStyle> styles = new HashMap<>();
		DataFormat df = wb.createDataFormat();
		CellStyle style;
		Font headerFont = wb.createFont();
		headerFont.setBold(true);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setFont(headerFont);
		styles.put("header", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setFont(headerFont);
		style.setDataFormat(df.getFormat("d-mmm"));
		styles.put("header_date", style);
		Font font1 = wb.createFont();
		font1.setBold(true);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.LEFT);
		style.setFont(font1);
		styles.put("cell_b", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setFont(font1);
		styles.put("cell_b_centered", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.RIGHT);
		style.setFont(font1);
		style.setDataFormat(df.getFormat("d-mmm"));
		styles.put("cell_b_date", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.RIGHT);
		style.setFont(font1);
		style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setDataFormat(df.getFormat("d-mmm"));
		styles.put("cell_g", style);
		Font font2 = wb.createFont();
		font2.setColor(IndexedColors.BLUE.getIndex());
		font2.setBold(true);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.LEFT);
		style.setFont(font2);
		styles.put("cell_bb", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.RIGHT);
		style.setFont(font1);
		style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		style.setDataFormat(df.getFormat("d-mmm"));
		styles.put("cell_bg", style);
		Font font3 = wb.createFont();
		font3.setFontHeightInPoints(((short) (14)));
		font3.setColor(IndexedColors.DARK_BLUE.getIndex());
		font3.setBold(true);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.LEFT);
		style.setFont(font3);
		style.setWrapText(true);
		styles.put("cell_h", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.LEFT);
		style.setWrapText(true);
		styles.put("cell_normal", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setWrapText(true);
		styles.put("cell_normal_centered", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.RIGHT);
		style.setWrapText(true);
		style.setDataFormat(df.getFormat("d-mmm"));
		styles.put("cell_normal_date", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setAlignment(HorizontalAlignment.LEFT);
		style.setIndention(((short) (1)));
		style.setWrapText(true);
		styles.put("cell_indented", style);
		style = BusinessPlan.createBorderedStyle(wb);
		style.setFillForegroundColor(IndexedColors.BLUE.getIndex());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		styles.put("cell_blue", style);
		return styles;
	}

	private static CellStyle createBorderedStyle(Workbook wb) {
		BorderStyle thin = BorderStyle.THIN;
		short black = IndexedColors.BLACK.getIndex();
		CellStyle style = wb.createCellStyle();
		style.setBorderRight(thin);
		style.setRightBorderColor(black);
		style.setBorderBottom(thin);
		style.setBottomBorderColor(black);
		style.setBorderLeft(thin);
		style.setLeftBorderColor(black);
		style.setBorderTop(thin);
		style.setTopBorderColor(black);
		return style;
	}
}

