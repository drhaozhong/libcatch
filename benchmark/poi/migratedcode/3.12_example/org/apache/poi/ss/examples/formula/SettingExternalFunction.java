package org.apache.poi.ss.examples.formula;


import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.ss.formula.OperationEvaluationContext;
import org.apache.poi.ss.formula.eval.ErrorEval;
import org.apache.poi.ss.formula.eval.ValueEval;
import org.apache.poi.ss.formula.functions.FreeRefFunction;
import org.apache.poi.ss.formula.udf.UDFFinder;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class SettingExternalFunction {
	public static class BloombergAddIn implements UDFFinder {
		private final Map<String, FreeRefFunction> _functionsByName;

		public BloombergAddIn() {
			FreeRefFunction NA = new FreeRefFunction() {
				public ValueEval evaluate(ValueEval[] args, OperationEvaluationContext ec) {
					return ErrorEval.NA;
				}
			};
			_functionsByName = new HashMap<String, FreeRefFunction>();
			_functionsByName.put("BDP", NA);
			_functionsByName.put("BDH", NA);
			_functionsByName.put("BDS", NA);
		}

		public FreeRefFunction findFunction(String name) {
			return _functionsByName.get(name.toUpperCase());
		}
	}

	public static void main(String[] args) throws IOException {
		Workbook wb = new XSSFWorkbook();
		wb.addToolPack(new SettingExternalFunction.BloombergAddIn());
		Sheet sheet = wb.createSheet();
		Row row = sheet.createRow(0);
		row.createCell(0).setCellFormula("BDP(\"GOOG Equity\",\"CHG_PCT_YTD\")/100");
		row.createCell(1).setCellFormula("BDH(\"goog us equity\",\"EBIT\",\"1/1/2005\",\"12/31/2009\",\"per=cy\",\"curr=USD\") ");
		row.createCell(2).setCellFormula("BDS(\"goog us equity\",\"top_20_holders_public_filings\") ");
		FileOutputStream out = new FileOutputStream("bloomberg-demo.xlsx");
		wb.write(out);
		out.close();
	}
}

