package org.apache.poi.xssf.usermodel.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.TableStyleInfo;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFTable;
import org.apache.poi.xssf.usermodel.XSSFTableColumn;
import org.apache.poi.xssf.usermodel.XSSFTableStyleInfo;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTTable;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTTableStyleInfo;


public class CreateTable {
	public static void main(String[] args) throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			XSSFSheet sheet = ((XSSFSheet) (wb.createSheet()));
			XSSFTable table = sheet.createTable();
			table.setName("Test");
			table.setDisplayName("Test_Table");
			table.getCTTable().addNewTableStyleInfo();
			table.getCTTable().getTableStyleInfo().setName("TableStyleMedium2");
			XSSFTableStyleInfo style = ((XSSFTableStyleInfo) (table.getStyle()));
			style.setName("TableStyleMedium2");
			style.setShowColumnStripes(false);
			style.setShowRowStripes(true);
			style.setFirstColumn(false);
			style.setLastColumn(false);
			style.setShowRowStripes(true);
			style.setShowColumnStripes(true);
			XSSFRow row;
			XSSFCell cell;
			for (int i = 0; i < 3; i++) {
				row = sheet.createRow(i);
				for (int j = 0; j < 3; j++) {
					cell = row.createCell(j);
					if (i == 0) {
						cell.setCellValue(("Column" + (j + 1)));
					}else {
						cell.setCellValue(((i + 1) * (j + 1)));
					}
				}
			}
			table.createColumn("Column 1");
			table.createColumn("Column 2");
			table.createColumn("Column 3");
			AreaReference reference = wb.getCreationHelper().createAreaReference(new CellReference(0, 0), new CellReference(2, 2));
			table.setCellReferences(reference);
			try (FileOutputStream fileOut = new FileOutputStream("ooxml-table.xlsx")) {
				wb.write(fileOut);
			}
		}
	}
}

