package org.apache.poi.ss.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataValidation;
import org.apache.poi.ss.usermodel.DataValidationConstraint;
import org.apache.poi.ss.usermodel.DataValidationHelper;
import org.apache.poi.ss.usermodel.Name;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class LinkedDropDownLists {
	LinkedDropDownLists(String workbookName) throws IOException {
		try (Workbook workbook = (workbookName.endsWith(".xlsx")) ? new XSSFWorkbook() : new HSSFWorkbook()) {
			Sheet sheet = workbook.createSheet("Linked Validations");
			LinkedDropDownLists.buildDataSheet(sheet);
			CellRangeAddressList addressList = new CellRangeAddressList(0, 0, 0, 0);
			DataValidationHelper dvHelper = sheet.getDataValidationHelper();
			DataValidationConstraint dvConstraint = dvHelper.createFormulaListConstraint("CHOICES");
			DataValidation validation = dvHelper.createValidation(dvConstraint, addressList);
			sheet.addValidationData(validation);
			addressList = new CellRangeAddressList(0, 0, 1, 1);
			dvConstraint = dvHelper.createFormulaListConstraint("INDIRECT(UPPER($A$1))");
			validation = dvHelper.createValidation(dvConstraint, addressList);
			sheet.addValidationData(validation);
			try (FileOutputStream fos = new FileOutputStream(workbookName)) {
				workbook.write(fos);
			}
		}
	}

	private static void buildDataSheet(Sheet dataSheet) {
		Row row = null;
		Cell cell = null;
		Name name = null;
		row = dataSheet.createRow(10);
		cell = row.createCell(0);
		cell.setCellValue("Animal");
		cell = row.createCell(1);
		cell.setCellValue("Vegetable");
		cell = row.createCell(2);
		cell.setCellValue("Mineral");
		name = dataSheet.getWorkbook().createName();
		name.setRefersToFormula("$A$11:$C$11");
		name.setNameName("CHOICES");
		row = dataSheet.createRow(11);
		cell = row.createCell(0);
		cell.setCellValue("Lion");
		cell = row.createCell(1);
		cell.setCellValue("Tiger");
		cell = row.createCell(2);
		cell.setCellValue("Leopard");
		cell = row.createCell(3);
		cell.setCellValue("Elephant");
		cell = row.createCell(4);
		cell.setCellValue("Eagle");
		cell = row.createCell(5);
		cell.setCellValue("Horse");
		cell = row.createCell(6);
		cell.setCellValue("Zebra");
		name = dataSheet.getWorkbook().createName();
		name.setRefersToFormula("$A$12:$G$12");
		name.setNameName("ANIMAL");
		row = dataSheet.createRow(12);
		cell = row.createCell(0);
		cell.setCellValue("Cabbage");
		cell = row.createCell(1);
		cell.setCellValue("Cauliflower");
		cell = row.createCell(2);
		cell.setCellValue("Potato");
		cell = row.createCell(3);
		cell.setCellValue("Onion");
		cell = row.createCell(4);
		cell.setCellValue("Beetroot");
		cell = row.createCell(5);
		cell.setCellValue("Asparagus");
		cell = row.createCell(6);
		cell.setCellValue("Spinach");
		cell = row.createCell(7);
		cell.setCellValue("Chard");
		name = dataSheet.getWorkbook().createName();
		name.setRefersToFormula("$A$13:$H$13");
		name.setNameName("VEGETABLE");
		row = dataSheet.createRow(13);
		cell = row.createCell(0);
		cell.setCellValue("Bauxite");
		cell = row.createCell(1);
		cell.setCellValue("Quartz");
		cell = row.createCell(2);
		cell.setCellValue("Feldspar");
		cell = row.createCell(3);
		cell.setCellValue("Shist");
		cell = row.createCell(4);
		cell.setCellValue("Shale");
		cell = row.createCell(5);
		cell.setCellValue("Mica");
		name = dataSheet.getWorkbook().createName();
		name.setRefersToFormula("$A$14:$F$14");
		name.setNameName("MINERAL");
	}
}

