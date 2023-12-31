package org.apache.poi.ss.examples;


import java.io.File;
import java.io.PrintStream;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Color;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.ExtendedColor;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;


public class CellStyleDetails {
	public static void main(String[] args) throws Exception {
		if ((args.length) == 0) {
			throw new IllegalArgumentException("Filename must be given");
		}
		Workbook wb = WorkbookFactory.create(new File(args[0]));
		DataFormatter formatter = new DataFormatter();
		for (int sn = 0; sn < (wb.getNumberOfSheets()); sn++) {
			Sheet sheet = wb.getSheetAt(sn);
			System.out.println(((("Sheet #" + sn) + " : ") + (sheet.getSheetName())));
			for (Row row : sheet) {
				System.out.println(("  Row " + (row.getRowNum())));
				for (Cell cell : row) {
					CellReference ref = new CellReference(cell);
					System.out.print(("    " + (ref.formatAsString())));
					System.out.print(((" (" + (cell.getColumnIndex())) + ") "));
					CellStyle style = cell.getCellStyle();
					System.out.print((("Format=" + (style.getDataFormatString())) + " "));
					System.out.print((("FG=" + (CellStyleDetails.renderColor(style.getFillForegroundColorColor()))) + " "));
					System.out.print((("BG=" + (CellStyleDetails.renderColor(style.getFillBackgroundColorColor()))) + " "));
					Font font = wb.getFontAt(style.getFontIndex());
					System.out.print((("Font=" + (font.getFontName())) + " "));
					System.out.print("FontColor=");
					if (font instanceof HSSFFont) {
						System.out.print(CellStyleDetails.renderColor(((HSSFFont) (font)).getHSSFColor(((HSSFWorkbook) (wb)))));
					}
					if (font instanceof XSSFFont) {
						System.out.print(CellStyleDetails.renderColor(((XSSFFont) (font)).getXSSFColor()));
					}
					System.out.println();
					System.out.println(("        " + (formatter.formatCellValue(cell))));
				}
			}
			System.out.println();
		}
		wb.close();
	}

	private static String renderColor(Color color) {
		if (color instanceof HSSFColor) {
			return ((HSSFColor) (color)).getHexString();
		}else
			if (color instanceof XSSFColor) {
				return ((XSSFColor) (color)).getARGBHex();
			}else {
				return "(none)";
			}

	}
}

