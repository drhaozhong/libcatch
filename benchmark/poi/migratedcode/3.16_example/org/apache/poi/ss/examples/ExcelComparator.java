package org.apache.poi.ss.examples;


import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Color;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.ExtendedColor;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;


public class ExcelComparator {
	private static final String CELL_DATA_DOES_NOT_MATCH = "Cell Data does not Match ::";

	private static final String CELL_FONT_ATTRIBUTES_DOES_NOT_MATCH = "Cell Font Attributes does not Match ::";

	private static class Locator {
		Workbook workbook;

		Sheet sheet;

		Row row;

		Cell cell;
	}

	List<String> listOfDifferences = new ArrayList<String>();

	public static void main(String[] args) throws Exception {
		if ((((args.length) != 2) || (!(new File(args[0]).exists()))) || (!(new File(args[1]).exists()))) {
			System.err.println((("java -cp <classpath> " + (ExcelComparator.class.getCanonicalName())) + " <workbook1.xls/x> <workbook2.xls/x"));
			System.exit((-1));
		}
		Workbook wb1 = WorkbookFactory.create(new File(args[0]));
		Workbook wb2 = WorkbookFactory.create(new File(args[1]));
		for (String d : ExcelComparator.compare(wb1, wb2)) {
			System.out.println(d);
		}
		wb2.close();
		wb1.close();
	}

	public static List<String> compare(Workbook wb1, Workbook wb2) {
		ExcelComparator.Locator loc1 = new ExcelComparator.Locator();
		ExcelComparator.Locator loc2 = new ExcelComparator.Locator();
		loc1.workbook = wb1;
		loc2.workbook = wb2;
		ExcelComparator excelComparator = new ExcelComparator();
		excelComparator.compareNumberOfSheets(loc1, loc2);
		excelComparator.compareSheetNames(loc1, loc2);
		excelComparator.compareSheetData(loc1, loc2);
		return excelComparator.listOfDifferences;
	}

	private void compareDataInAllSheets(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		for (int i = 0; i < (loc1.workbook.getNumberOfSheets()); i++) {
			if ((loc2.workbook.getNumberOfSheets()) <= i)
				return;

			loc1.sheet = loc1.workbook.getSheetAt(i);
			loc2.sheet = loc2.workbook.getSheetAt(i);
			compareDataInSheet(loc1, loc2);
		}
	}

	private void compareDataInSheet(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		for (int j = 0; j < (loc1.sheet.getPhysicalNumberOfRows()); j++) {
			if ((loc2.sheet.getPhysicalNumberOfRows()) <= j)
				return;

			loc1.row = loc1.sheet.getRow(j);
			loc2.row = loc2.sheet.getRow(j);
			if (((loc1.row) == null) || ((loc2.row) == null)) {
				continue;
			}
			compareDataInRow(loc1, loc2);
		}
	}

	private void compareDataInRow(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		for (int k = 0; k < (loc1.row.getLastCellNum()); k++) {
			if ((loc2.row.getPhysicalNumberOfCells()) <= k)
				return;

			loc1.cell = loc1.row.getCell(k);
			loc2.cell = loc2.row.getCell(k);
			if (((loc1.cell) == null) || ((loc2.cell) == null)) {
				continue;
			}
			compareDataInCell(loc1, loc2);
		}
	}

	private void compareDataInCell(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		if (isCellTypeMatches(loc1, loc2)) {
			final CellType loc1cellType = loc1.cell.getCellTypeEnum();
			switch (loc1cellType) {
				case BLANK :
				case STRING :
				case ERROR :
					isCellContentMatches(loc1, loc2);
					break;
				case BOOLEAN :
					isCellContentMatchesForBoolean(loc1, loc2);
					break;
				case FORMULA :
					isCellContentMatchesForFormula(loc1, loc2);
					break;
				case NUMERIC :
					if (DateUtil.isCellDateFormatted(loc1.cell)) {
						isCellContentMatchesForDate(loc1, loc2);
					}else {
						isCellContentMatchesForNumeric(loc1, loc2);
					}
					break;
				default :
					throw new IllegalStateException(("Unexpected cell type: " + loc1cellType));
			}
		}
		isCellFillPatternMatches(loc1, loc2);
		isCellAlignmentMatches(loc1, loc2);
		isCellHiddenMatches(loc1, loc2);
		isCellLockedMatches(loc1, loc2);
		isCellFontFamilyMatches(loc1, loc2);
		isCellFontSizeMatches(loc1, loc2);
		isCellFontBoldMatches(loc1, loc2);
		isCellUnderLineMatches(loc1, loc2);
		isCellFontItalicsMatches(loc1, loc2);
		isCellBorderMatches(loc1, loc2, 't');
		isCellBorderMatches(loc1, loc2, 'l');
		isCellBorderMatches(loc1, loc2, 'b');
		isCellBorderMatches(loc1, loc2, 'r');
		isCellFillBackGroundMatches(loc1, loc2);
	}

	private void compareNumberOfColumnsInSheets(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		for (int i = 0; i < (loc1.workbook.getNumberOfSheets()); i++) {
			if ((loc2.workbook.getNumberOfSheets()) <= i)
				return;

			loc1.sheet = loc1.workbook.getSheetAt(i);
			loc2.sheet = loc2.workbook.getSheetAt(i);
			Iterator<Row> ri1 = loc1.sheet.rowIterator();
			Iterator<Row> ri2 = loc2.sheet.rowIterator();
			int num1 = (ri1.hasNext()) ? ri1.next().getPhysicalNumberOfCells() : 0;
			int num2 = (ri2.hasNext()) ? ri2.next().getPhysicalNumberOfCells() : 0;
			if (num1 != num2) {
				String str = String.format(Locale.ROOT, "%s\nworkbook1 -> %s [%d] != workbook2 -> %s [%d]", "Number Of Columns does not Match ::", loc1.sheet.getSheetName(), num1, loc2.sheet.getSheetName(), num2);
				listOfDifferences.add(str);
			}
		}
	}

	private void compareNumberOfRowsInSheets(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		for (int i = 0; i < (loc1.workbook.getNumberOfSheets()); i++) {
			if ((loc2.workbook.getNumberOfSheets()) <= i)
				return;

			loc1.sheet = loc1.workbook.getSheetAt(i);
			loc2.sheet = loc2.workbook.getSheetAt(i);
			int num1 = loc1.sheet.getPhysicalNumberOfRows();
			int num2 = loc2.sheet.getPhysicalNumberOfRows();
			if (num1 != num2) {
				String str = String.format(Locale.ROOT, "%s\nworkbook1 -> %s [%d] != workbook2 -> %s [%d]", "Number Of Rows does not Match ::", loc1.sheet.getSheetName(), num1, loc2.sheet.getSheetName(), num2);
				listOfDifferences.add(str);
			}
		}
	}

	private void compareNumberOfSheets(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		int num1 = loc1.workbook.getNumberOfSheets();
		int num2 = loc2.workbook.getNumberOfSheets();
		if (num1 != num2) {
			String str = String.format(Locale.ROOT, "%s\nworkbook1 [%d] != workbook2 [%d]", "Number of Sheets do not match ::", num1, num2);
			listOfDifferences.add(str);
		}
	}

	private void compareSheetData(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		compareNumberOfRowsInSheets(loc1, loc2);
		compareNumberOfColumnsInSheets(loc1, loc2);
		compareDataInAllSheets(loc1, loc2);
	}

	private void compareSheetNames(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		for (int i = 0; i < (loc1.workbook.getNumberOfSheets()); i++) {
			String name1 = loc1.workbook.getSheetName(i);
			String name2 = ((loc2.workbook.getNumberOfSheets()) > i) ? loc2.workbook.getSheetName(i) : "";
			if (!(name1.equals(name2))) {
				String str = String.format(Locale.ROOT, "%s\nworkbook1 -> %s [%d] != workbook2 -> %s [%d]", "Name of the sheets do not match ::", name1, (i + 1), name2, (i + 1));
				listOfDifferences.add(str);
			}
		}
	}

	private void addMessage(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2, String messageStart, String value1, String value2) {
		String str = String.format(Locale.ROOT, "%s\nworkbook1 -> %s -> %s [%s] != workbook2 -> %s -> %s [%s]", messageStart, loc1.sheet.getSheetName(), new CellReference(loc1.cell).formatAsString(), value1, loc2.sheet.getSheetName(), new CellReference(loc2.cell).formatAsString(), value2);
		listOfDifferences.add(str);
	}

	private void isCellAlignmentMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		short align1 = loc1.cell.getCellStyle().getAlignment();
		short align2 = loc2.cell.getCellStyle().getAlignment();
		if (align1 != align2) {
			addMessage(loc1, loc2, "Cell Alignment does not Match ::", Short.toString(align1), Short.toString(align2));
		}
	}

	private void isCellBorderMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2, char borderSide) {
		if (!((loc1.cell) instanceof XSSFCell))
			return;

		XSSFCellStyle style1 = ((XSSFCell) (loc1.cell)).getCellStyle();
		XSSFCellStyle style2 = ((XSSFCell) (loc2.cell)).getCellStyle();
		boolean b1;
		boolean b2;
		String borderName;
		switch (borderSide) {
			case 't' :
			default :
				b1 = (style1.getBorderTopEnum()) == (BorderStyle.THIN);
				b2 = (style2.getBorderTopEnum()) == (BorderStyle.THIN);
				borderName = "TOP";
				break;
			case 'b' :
				b1 = (style1.getBorderBottomEnum()) == (BorderStyle.THIN);
				b2 = (style2.getBorderBottomEnum()) == (BorderStyle.THIN);
				borderName = "BOTTOM";
				break;
			case 'l' :
				b1 = (style1.getBorderLeftEnum()) == (BorderStyle.THIN);
				b2 = (style2.getBorderLeftEnum()) == (BorderStyle.THIN);
				borderName = "LEFT";
				break;
			case 'r' :
				b1 = (style1.getBorderRightEnum()) == (BorderStyle.THIN);
				b2 = (style2.getBorderRightEnum()) == (BorderStyle.THIN);
				borderName = "RIGHT";
				break;
		}
		if (b1 != b2) {
			addMessage(loc1, loc2, "Cell Border Attributes does not Match ::", (((b1 ? "" : "NOT ") + borderName) + " BORDER"), (((b2 ? "" : "NOT ") + borderName) + " BORDER"));
		}
	}

	private void isCellContentMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		String str1 = loc1.cell.getRichStringCellValue().getString();
		String str2 = loc2.cell.getRichStringCellValue().getString();
		if (!(str1.equals(str2))) {
			addMessage(loc1, loc2, ExcelComparator.CELL_DATA_DOES_NOT_MATCH, str1, str2);
		}
	}

	private void isCellContentMatchesForBoolean(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		boolean b1 = loc1.cell.getBooleanCellValue();
		boolean b2 = loc2.cell.getBooleanCellValue();
		if (b1 != b2) {
			addMessage(loc1, loc2, ExcelComparator.CELL_DATA_DOES_NOT_MATCH, Boolean.toString(b1), Boolean.toString(b2));
		}
	}

	private void isCellContentMatchesForDate(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		Date date1 = loc1.cell.getDateCellValue();
		Date date2 = loc2.cell.getDateCellValue();
		if (!(date1.equals(date2))) {
			addMessage(loc1, loc2, ExcelComparator.CELL_DATA_DOES_NOT_MATCH, date1.toGMTString(), date2.toGMTString());
		}
	}

	private void isCellContentMatchesForFormula(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		String form1 = loc1.cell.getCellFormula();
		String form2 = loc2.cell.getCellFormula();
		if (!(form1.equals(form2))) {
			addMessage(loc1, loc2, ExcelComparator.CELL_DATA_DOES_NOT_MATCH, form1, form2);
		}
	}

	private void isCellContentMatchesForNumeric(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		double num1 = loc1.cell.getNumericCellValue();
		double num2 = loc2.cell.getNumericCellValue();
		if (num1 != num2) {
			addMessage(loc1, loc2, ExcelComparator.CELL_DATA_DOES_NOT_MATCH, Double.toString(num1), Double.toString(num2));
		}
	}

	private String getCellFillBackground(ExcelComparator.Locator loc) {
		Color col = loc.cell.getCellStyle().getFillForegroundColorColor();
		return col instanceof XSSFColor ? ((XSSFColor) (col)).getARGBHex() : "NO COLOR";
	}

	private void isCellFillBackGroundMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		String col1 = getCellFillBackground(loc1);
		String col2 = getCellFillBackground(loc2);
		if (!(col1.equals(col2))) {
			addMessage(loc1, loc2, "Cell Fill Color does not Match ::", col1, col2);
		}
	}

	private void isCellFillPatternMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		short fill1 = loc1.cell.getCellStyle().getFillPattern();
		short fill2 = loc2.cell.getCellStyle().getFillPattern();
		if (fill1 != fill2) {
			addMessage(loc1, loc2, "Cell Fill pattern does not Match ::", Short.toString(fill1), Short.toString(fill2));
		}
	}

	private void isCellFontBoldMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		if (!((loc1.cell) instanceof XSSFCell))
			return;

		boolean b1 = ((XSSFCell) (loc1.cell)).getCellStyle().getFont().getBold();
		boolean b2 = ((XSSFCell) (loc2.cell)).getCellStyle().getFont().getBold();
		if (b1 != b2) {
			addMessage(loc1, loc2, ExcelComparator.CELL_FONT_ATTRIBUTES_DOES_NOT_MATCH, ((b1 ? "" : "NOT ") + "BOLD"), ((b2 ? "" : "NOT ") + "BOLD"));
		}
	}

	private void isCellFontFamilyMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		if (!((loc1.cell) instanceof XSSFCell))
			return;

		String family1 = ((XSSFCell) (loc1.cell)).getCellStyle().getFont().getFontName();
		String family2 = ((XSSFCell) (loc2.cell)).getCellStyle().getFont().getFontName();
		if (!(family1.equals(family2))) {
			addMessage(loc1, loc2, "Cell Font Family does not Match ::", family1, family2);
		}
	}

	private void isCellFontItalicsMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		if (!((loc1.cell) instanceof XSSFCell))
			return;

		boolean b1 = ((XSSFCell) (loc1.cell)).getCellStyle().getFont().getItalic();
		boolean b2 = ((XSSFCell) (loc2.cell)).getCellStyle().getFont().getItalic();
		if (b1 != b2) {
			addMessage(loc1, loc2, ExcelComparator.CELL_FONT_ATTRIBUTES_DOES_NOT_MATCH, ((b1 ? "" : "NOT ") + "ITALICS"), ((b2 ? "" : "NOT ") + "ITALICS"));
		}
	}

	private void isCellFontSizeMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		if (!((loc1.cell) instanceof XSSFCell))
			return;

		short size1 = ((XSSFCell) (loc1.cell)).getCellStyle().getFont().getFontHeightInPoints();
		short size2 = ((XSSFCell) (loc2.cell)).getCellStyle().getFont().getFontHeightInPoints();
		if (size1 != size2) {
			addMessage(loc1, loc2, "Cell Font Size does not Match ::", Short.toString(size1), Short.toString(size2));
		}
	}

	private void isCellHiddenMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		boolean b1 = loc1.cell.getCellStyle().getHidden();
		boolean b2 = loc1.cell.getCellStyle().getHidden();
		if (b1 != b2) {
			addMessage(loc1, loc2, "Cell Visibility does not Match ::", ((b1 ? "" : "NOT ") + "HIDDEN"), ((b2 ? "" : "NOT ") + "HIDDEN"));
		}
	}

	private void isCellLockedMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		boolean b1 = loc1.cell.getCellStyle().getLocked();
		boolean b2 = loc1.cell.getCellStyle().getLocked();
		if (b1 != b2) {
			addMessage(loc1, loc2, "Cell Protection does not Match ::", ((b1 ? "" : "NOT ") + "LOCKED"), ((b2 ? "" : "NOT ") + "LOCKED"));
		}
	}

	private boolean isCellTypeMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		CellType type1 = loc1.cell.getCellTypeEnum();
		CellType type2 = loc2.cell.getCellTypeEnum();
		if (type1 == type2)
			return true;

		addMessage(loc1, loc2, "Cell Data-Type does not Match in :: ", type1.name(), type2.name());
		return false;
	}

	private void isCellUnderLineMatches(ExcelComparator.Locator loc1, ExcelComparator.Locator loc2) {
		if (!((loc1.cell) instanceof XSSFCell))
			return;

		byte b1 = ((XSSFCell) (loc1.cell)).getCellStyle().getFont().getUnderline();
		byte b2 = ((XSSFCell) (loc2.cell)).getCellStyle().getFont().getUnderline();
		if (b1 != b2) {
			addMessage(loc1, loc2, ExcelComparator.CELL_FONT_ATTRIBUTES_DOES_NOT_MATCH, ((b1 == 1 ? "" : "NOT ") + "UNDERLINE"), ((b2 == 1 ? "" : "NOT ") + "UNDERLINE"));
		}
	}
}

