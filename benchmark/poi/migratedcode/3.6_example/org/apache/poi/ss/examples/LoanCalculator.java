package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Name;
import org.apache.poi.ss.usermodel.PrintSetup;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class LoanCalculator {
	public static void main(String[] args) throws Exception {
		Workbook wb;
		if (((args.length) > 0) && (args[0].equals("-xls")))
			wb = new HSSFWorkbook();
		else
			wb = new XSSFWorkbook();

		Map<String, CellStyle> styles = LoanCalculator.createStyles(wb);
		Sheet sheet = wb.createSheet("Loan Calculator");
		sheet.setPrintGridlines(false);
		sheet.setDisplayGridlines(false);
		PrintSetup printSetup = sheet.getPrintSetup();
		printSetup.setLandscape(true);
		sheet.setFitToPage(true);
		sheet.setHorizontallyCenter(true);
		sheet.setColumnWidth(0, (3 * 256));
		sheet.setColumnWidth(1, (3 * 256));
		sheet.setColumnWidth(2, (11 * 256));
		sheet.setColumnWidth(3, (14 * 256));
		sheet.setColumnWidth(4, (14 * 256));
		sheet.setColumnWidth(5, (14 * 256));
		sheet.setColumnWidth(6, (14 * 256));
		LoanCalculator.createNames(wb);
		Row titleRow = sheet.createRow(0);
		titleRow.setHeightInPoints(35);
		for (int i = 1; i <= 7; i++) {
			titleRow.createCell(i).setCellStyle(styles.get("title"));
		}
		Cell titleCell = titleRow.getCell(2);
		titleCell.setCellValue("Simple Loan Calculator");
		sheet.addMergedRegion(CellRangeAddress.valueOf("$C$1:$H$1"));
		Row row = sheet.createRow(2);
		Cell cell = row.createCell(4);
		cell.setCellValue("Enter values");
		cell.setCellStyle(styles.get("item_right"));
		row = sheet.createRow(3);
		cell = row.createCell(2);
		cell.setCellValue("Loan amount");
		cell.setCellStyle(styles.get("item_left"));
		cell = row.createCell(4);
		cell.setCellStyle(styles.get("input_$"));
		cell.setAsActiveCell();
		row = sheet.createRow(4);
		cell = row.createCell(2);
		cell.setCellValue("Annual interest rate");
		cell.setCellStyle(styles.get("item_left"));
		cell = row.createCell(4);
		cell.setCellStyle(styles.get("input_%"));
		row = sheet.createRow(5);
		cell = row.createCell(2);
		cell.setCellValue("Loan period in years");
		cell.setCellStyle(styles.get("item_left"));
		cell = row.createCell(4);
		cell.setCellStyle(styles.get("input_i"));
		row = sheet.createRow(6);
		cell = row.createCell(2);
		cell.setCellValue("Start date of loan");
		cell.setCellStyle(styles.get("item_left"));
		cell = row.createCell(4);
		cell.setCellStyle(styles.get("input_d"));
		row = sheet.createRow(8);
		cell = row.createCell(2);
		cell.setCellValue("Monthly payment");
		cell.setCellStyle(styles.get("item_left"));
		cell = row.createCell(4);
		cell.setCellFormula("IF(Values_Entered,Monthly_Payment,\"\")");
		cell.setCellStyle(styles.get("formula_$"));
		row = sheet.createRow(9);
		cell = row.createCell(2);
		cell.setCellValue("Number of payments");
		cell.setCellStyle(styles.get("item_left"));
		cell = row.createCell(4);
		cell.setCellFormula("IF(Values_Entered,Loan_Years*12,\"\")");
		cell.setCellStyle(styles.get("formula_i"));
		row = sheet.createRow(10);
		cell = row.createCell(2);
		cell.setCellValue("Total interest");
		cell.setCellStyle(styles.get("item_left"));
		cell = row.createCell(4);
		cell.setCellFormula("IF(Values_Entered,Total_Cost-Loan_Amount,\"\")");
		cell.setCellStyle(styles.get("formula_$"));
		row = sheet.createRow(11);
		cell = row.createCell(2);
		cell.setCellValue("Total cost of loan");
		cell.setCellStyle(styles.get("item_left"));
		cell = row.createCell(4);
		cell.setCellFormula("IF(Values_Entered,Monthly_Payment*Number_of_Payments,\"\")");
		cell.setCellStyle(styles.get("formula_$"));
		String file = "loan-calculator.xls";
		if (wb instanceof XSSFWorkbook)
			file += "x";

		FileOutputStream out = new FileOutputStream(file);
		wb.write(out);
		out.close();
	}

	private static Map<String, CellStyle> createStyles(Workbook wb) {
		Map<String, CellStyle> styles = new HashMap<String, CellStyle>();
		CellStyle style;
		Font titleFont = wb.createFont();
		titleFont.setFontHeightInPoints(((short) (14)));
		titleFont.setFontName("Trebuchet MS");
		style = wb.createCellStyle();
		style.setFont(titleFont);
		style.setBorderBottom(CellStyle.BORDER_DOTTED);
		style.setBottomBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		styles.put("title", style);
		Font itemFont = wb.createFont();
		itemFont.setFontHeightInPoints(((short) (9)));
		itemFont.setFontName("Trebuchet MS");
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_LEFT);
		style.setFont(itemFont);
		styles.put("item_left", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_RIGHT);
		style.setFont(itemFont);
		styles.put("item_right", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_RIGHT);
		style.setFont(itemFont);
		style.setBorderRight(CellStyle.BORDER_DOTTED);
		style.setRightBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderBottom(CellStyle.BORDER_DOTTED);
		style.setBottomBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderLeft(CellStyle.BORDER_DOTTED);
		style.setLeftBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderTop(CellStyle.BORDER_DOTTED);
		style.setTopBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setDataFormat(wb.createDataFormat().getFormat("_($* #,##0.00_);_($* (#,##0.00);_($* \"-\"??_);_(@_)"));
		styles.put("input_$", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_RIGHT);
		style.setFont(itemFont);
		style.setBorderRight(CellStyle.BORDER_DOTTED);
		style.setRightBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderBottom(CellStyle.BORDER_DOTTED);
		style.setBottomBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderLeft(CellStyle.BORDER_DOTTED);
		style.setLeftBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderTop(CellStyle.BORDER_DOTTED);
		style.setTopBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setDataFormat(wb.createDataFormat().getFormat("0.000%"));
		styles.put("input_%", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_RIGHT);
		style.setFont(itemFont);
		style.setBorderRight(CellStyle.BORDER_DOTTED);
		style.setRightBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderBottom(CellStyle.BORDER_DOTTED);
		style.setBottomBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderLeft(CellStyle.BORDER_DOTTED);
		style.setLeftBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderTop(CellStyle.BORDER_DOTTED);
		style.setTopBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setDataFormat(wb.createDataFormat().getFormat("0"));
		styles.put("input_i", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setFont(itemFont);
		style.setDataFormat(wb.createDataFormat().getFormat("m/d/yy"));
		styles.put("input_d", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_RIGHT);
		style.setFont(itemFont);
		style.setBorderRight(CellStyle.BORDER_DOTTED);
		style.setRightBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderBottom(CellStyle.BORDER_DOTTED);
		style.setBottomBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderLeft(CellStyle.BORDER_DOTTED);
		style.setLeftBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderTop(CellStyle.BORDER_DOTTED);
		style.setTopBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setDataFormat(wb.createDataFormat().getFormat("$##,##0.00"));
		style.setBorderBottom(CellStyle.BORDER_DOTTED);
		style.setBottomBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		styles.put("formula_$", style);
		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_RIGHT);
		style.setFont(itemFont);
		style.setBorderRight(CellStyle.BORDER_DOTTED);
		style.setRightBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderBottom(CellStyle.BORDER_DOTTED);
		style.setBottomBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderLeft(CellStyle.BORDER_DOTTED);
		style.setLeftBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setBorderTop(CellStyle.BORDER_DOTTED);
		style.setTopBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setDataFormat(wb.createDataFormat().getFormat("0"));
		style.setBorderBottom(CellStyle.BORDER_DOTTED);
		style.setBottomBorderColor(IndexedColors.GREY_40_PERCENT.getIndex());
		style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		styles.put("formula_i", style);
		return styles;
	}

	public static void createNames(Workbook wb) {
		Name name;
		name = wb.createName();
		name.setNameName("Interest_Rate");
		name.setRefersToFormula("'Loan Calculator'!$E$5");
		name = wb.createName();
		name.setNameName("Loan_Amount");
		name.setRefersToFormula("'Loan Calculator'!$E$4");
		name = wb.createName();
		name.setNameName("Loan_Start");
		name.setRefersToFormula("'Loan Calculator'!$E$7");
		name = wb.createName();
		name.setNameName("Loan_Years");
		name.setRefersToFormula("'Loan Calculator'!$E$6");
		name = wb.createName();
		name.setNameName("Number_of_Payments");
		name.setRefersToFormula("'Loan Calculator'!$E$10");
		name = wb.createName();
		name.setNameName("Monthly_Payment");
		name.setRefersToFormula("-PMT(Interest_Rate/12,Number_of_Payments,Loan_Amount)");
		name = wb.createName();
		name.setNameName("Total_Cost");
		name.setRefersToFormula("'Loan Calculator'!$E$12");
		name = wb.createName();
		name.setNameName("Total_Interest");
		name.setRefersToFormula("'Loan Calculator'!$E$11");
		name = wb.createName();
		name.setNameName("Values_Entered");
		name.setRefersToFormula("IF(Loan_Amount*Interest_Rate*Loan_Years*Loan_Start>0,1,0)");
	}
}

