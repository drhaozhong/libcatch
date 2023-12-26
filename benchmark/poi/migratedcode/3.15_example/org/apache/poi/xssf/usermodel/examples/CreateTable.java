package org.apache.poi.xssf.usermodel.examples;


import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFTable;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTTable;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTTableColumn;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTTableColumns;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTTableStyleInfo;


public class CreateTable {
	public static void main(String[] args) throws FileNotFoundException, IOException {
		Workbook wb = new XSSFWorkbook();
		XSSFSheet sheet = ((XSSFSheet) (wb.createSheet()));
		XSSFTable table = sheet.createTable();
		table.setDisplayName("Test");
		CTTable cttable = table.getCTTable();
		CTTableStyleInfo style = cttable.addNewTableStyleInfo();
		style.setName("TableStyleMedium2");
		style.setShowColumnStripes(false);
		style.setShowRowStripes(true);
		AreaReference reference = new AreaReference(new CellReference(0, 0), new CellReference(2, 2));
		cttable.setRef(reference.formatAsString());
		cttable.setId(1);
		cttable.setName("Test");
		cttable.setTotalsRowCount(1);
		CTTableColumns columns = cttable.addNewTableColumns();
		columns.setCount(3);
		CTTableColumn column;
		XSSFRow row;
		XSSFCell cell;
		for (int i = 0; i < 3; i++) {
			column = columns.addNewTableColumn();
			column.setName("Column");
			column.setId((i + 1));
			row = sheet.createRow(i);
			for (int j = 0; j < 3; j++) {
				cell = row.createCell(j);
				if (i == 0) {
					cell.setCellValue(("Column" + j));
				}else {
					cell.setCellValue("0");
				}
			}
		}
		FileOutputStream fileOut = new FileOutputStream("ooxml-table.xlsx");
		wb.write(fileOut);
		fileOut.close();
	}
}

