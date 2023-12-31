package org.apache.poi.hssf.usermodel.examples;


import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.util.POILogFactory;
import org.apache.poi.util.POILogger;


public class Outlines implements Closeable {
	public static void main(String[] args) throws IOException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		POILogger LOGGER = POILogFactory.getLogger(Outlines.class);
		for (int i = 1; i <= 13; i++) {
			Outlines o = new Outlines();
			try {
				String log = ((String) (Outlines.class.getDeclaredMethod(("test" + i)).invoke(o)));
				String filename = ("outline" + i) + ".xls";
				o.writeOut(filename);
				LOGGER.log(POILogger.INFO, ((filename + " written. ") + log));
			} finally {
				o.close();
			}
		}
	}

	private final HSSFWorkbook wb = new HSSFWorkbook();

	private final HSSFSheet sheet1 = wb.createSheet("new sheet");

	public void writeOut(String filename) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(filename);
		try {
			wb.write(fileOut);
		} finally {
			fileOut.close();
		}
	}

	public void close() throws IOException {
		wb.close();
	}

	public String test1() {
		sheet1.groupColumn(4, 7);
		for (int row = 0; row < 200; row++) {
			HSSFRow r = sheet1.createRow(row);
			for (int column = 0; column < 200; column++) {
				HSSFCell c = r.createCell(column);
				c.setCellValue(column);
			}
		}
		return "Two expanded groups.";
	}

	public String test2() {
		sheet1.groupColumn(2, 10);
		sheet1.groupColumn(4, 7);
		sheet1.setColumnGroupCollapsed(4, true);
		return "Two groups.  Inner group collapsed.";
	}

	public String test3() {
		sheet1.groupColumn(2, 10);
		sheet1.groupColumn(4, 7);
		sheet1.setColumnGroupCollapsed(4, true);
		sheet1.setColumnGroupCollapsed(2, true);
		return "Two groups.  Both collapsed.";
	}

	public String test4() {
		sheet1.groupColumn(2, 10);
		sheet1.groupColumn(4, 7);
		sheet1.setColumnGroupCollapsed(4, true);
		sheet1.setColumnGroupCollapsed(2, true);
		sheet1.setColumnGroupCollapsed(4, false);
		return "Two groups.  Collapsed then inner group expanded.";
	}

	public String test5() {
		sheet1.groupColumn(2, 10);
		sheet1.groupColumn(4, 7);
		sheet1.setColumnGroupCollapsed(4, true);
		sheet1.setColumnGroupCollapsed(2, true);
		sheet1.setColumnGroupCollapsed(4, false);
		sheet1.setColumnGroupCollapsed(3, false);
		return "Two groups.  Collapsed then reexpanded.";
	}

	public String test6() {
		sheet1.groupColumn(2, 10);
		sheet1.groupColumn(4, 10);
		sheet1.setColumnGroupCollapsed(4, true);
		sheet1.setColumnGroupCollapsed(2, true);
		sheet1.setColumnGroupCollapsed(3, false);
		return "Two groups with matching end points.  Second group collapsed.";
	}

	public String test7() {
		sheet1.groupRow(5, 14);
		sheet1.groupRow(7, 10);
		return "Row outlines.";
	}

	public String test8() {
		sheet1.groupRow(5, 14);
		sheet1.groupRow(7, 10);
		sheet1.setRowGroupCollapsed(7, true);
		return "Row outlines.  Inner group collapsed.";
	}

	public String test9() {
		sheet1.groupRow(5, 14);
		sheet1.groupRow(7, 10);
		sheet1.setRowGroupCollapsed(7, true);
		sheet1.setRowGroupCollapsed(5, true);
		return "Row outlines.  Both collapsed.";
	}

	public String test10() {
		sheet1.groupRow(5, 14);
		sheet1.groupRow(7, 10);
		sheet1.setRowGroupCollapsed(7, true);
		sheet1.setRowGroupCollapsed(5, true);
		sheet1.setRowGroupCollapsed(8, false);
		return "Row outlines.  Collapsed then inner group expanded.";
	}

	public String test11() {
		sheet1.groupRow(5, 14);
		sheet1.groupRow(7, 10);
		sheet1.setRowGroupCollapsed(7, true);
		sheet1.setRowGroupCollapsed(5, true);
		sheet1.setRowGroupCollapsed(8, false);
		sheet1.setRowGroupCollapsed(14, false);
		return "Row outlines.  Collapsed then expanded.";
	}

	public String test12() {
		sheet1.groupRow(5, 14);
		sheet1.groupRow(7, 14);
		sheet1.setRowGroupCollapsed(7, true);
		sheet1.setRowGroupCollapsed(5, true);
		sheet1.setRowGroupCollapsed(6, false);
		return "Row outlines.  Two row groups with matching end points.  Second group collapsed.";
	}

	public String test13() {
		sheet1.groupRow(5, 14);
		sheet1.groupRow(7, 14);
		sheet1.groupRow(16, 19);
		sheet1.groupColumn(4, 7);
		sheet1.groupColumn(9, 12);
		sheet1.groupColumn(10, 11);
		return "Mixed bag.";
	}
}

