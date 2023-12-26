package org.apache.poi.ss.examples.formula;


import java.io.File;
import java.io.PrintStream;
import org.apache.poi.ss.formula.functions.FreeRefFunction;
import org.apache.poi.ss.formula.udf.DefaultUDFFinder;
import org.apache.poi.ss.formula.udf.UDFFinder;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;


public class UserDefinedFunctionExample {
	public static void main(String[] args) throws Exception {
		if ((args.length) != 2) {
			System.out.println("usage: UserDefinedFunctionExample fileName cellId");
			return;
		}
		System.out.println(("fileName: " + (args[0])));
		System.out.println(("cell: " + (args[1])));
		File workbookFile = new File(args[0]);
		try (Workbook workbook = WorkbookFactory.create(workbookFile, null, true)) {
			String[] functionNames = new String[]{ "calculatePayment" };
			FreeRefFunction[] functionImpls = new FreeRefFunction[]{ new CalculateMortgage() };
			UDFFinder udfToolpack = new DefaultUDFFinder(functionNames, functionImpls);
			workbook.addToolPack(udfToolpack);
			FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
			CellReference cr = new CellReference(args[1]);
			String sheetName = cr.getSheetName();
			Sheet sheet = workbook.getSheet(sheetName);
			int rowIdx = cr.getRow();
			int colIdx = cr.getCol();
			Row row = sheet.getRow(rowIdx);
			Cell cell = row.getCell(colIdx);
			CellValue value = evaluator.evaluate(cell);
			System.out.println(("returns value: " + value));
		}
	}
}

