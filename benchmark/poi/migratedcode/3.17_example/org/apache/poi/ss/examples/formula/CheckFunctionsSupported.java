package org.apache.poi.ss.examples.formula;


import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.poi.ss.examples.formula.CheckFunctionsSupported.FormulaEvaluationProblems;
import org.apache.poi.ss.formula.eval.NotImplementedException;
import org.apache.poi.ss.formula.eval.NotImplementedFunctionException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;


public class CheckFunctionsSupported {
	public static void main(String[] args) throws Exception {
		if ((args.length) < 1) {
			System.err.println("Use:");
			System.err.println("  CheckFunctionsSupported <filename>");
			return;
		}
		Workbook wb = WorkbookFactory.create(new File(args[0]));
		CheckFunctionsSupported check = new CheckFunctionsSupported(wb);
		List<CheckFunctionsSupported.FormulaEvaluationProblems> problems = new ArrayList<CheckFunctionsSupported.FormulaEvaluationProblems>();
		for (int sn = 0; sn < (wb.getNumberOfSheets()); sn++) {
			problems.add(check.getEvaluationProblems(sn));
		}
		Set<String> unsupportedFunctions = new TreeSet<String>();
		for (CheckFunctionsSupported.FormulaEvaluationProblems p : problems) {
			unsupportedFunctions.addAll(p.unsupportedFunctions);
		}
		if (unsupportedFunctions.isEmpty()) {
			System.out.println("There are no unsupported formula functions used");
		}else {
			System.out.println("Unsupported formula functions:");
			for (String function : unsupportedFunctions) {
				System.out.println(("  " + function));
			}
			System.out.println(("Total unsupported functions = " + (unsupportedFunctions.size())));
		}
		for (int sn = 0; sn < (wb.getNumberOfSheets()); sn++) {
			String sheetName = wb.getSheetName(sn);
			CheckFunctionsSupported.FormulaEvaluationProblems probs = problems.get(sn);
			System.out.println();
			System.out.println(("Sheet = " + sheetName));
			if (probs.unevaluatableCells.isEmpty()) {
				System.out.println(" All cells evaluated without error");
			}else {
				for (CellReference cr : probs.unevaluatableCells.keySet()) {
					System.out.println((((" " + (cr.formatAsString())) + " - ") + (probs.unevaluatableCells.get(cr))));
				}
			}
		}
	}

	private Workbook workbook;

	private FormulaEvaluator evaluator;

	public CheckFunctionsSupported(Workbook workbook) {
		this.workbook = workbook;
		this.evaluator = workbook.getCreationHelper().createFormulaEvaluator();
	}

	public Set<String> getUnsupportedFunctions(String sheetName) {
		return getUnsupportedFunctions(workbook.getSheet(sheetName));
	}

	public Set<String> getUnsupportedFunctions(int sheetIndex) {
		return getUnsupportedFunctions(workbook.getSheetAt(sheetIndex));
	}

	public Set<String> getUnsupportedFunctions(Sheet sheet) {
		CheckFunctionsSupported.FormulaEvaluationProblems problems = getEvaluationProblems(sheet);
		return problems.unsupportedFunctions;
	}

	public CheckFunctionsSupported.FormulaEvaluationProblems getEvaluationProblems(String sheetName) {
		return getEvaluationProblems(workbook.getSheet(sheetName));
	}

	public CheckFunctionsSupported.FormulaEvaluationProblems getEvaluationProblems(int sheetIndex) {
		return getEvaluationProblems(workbook.getSheetAt(sheetIndex));
	}

	public CheckFunctionsSupported.FormulaEvaluationProblems getEvaluationProblems(Sheet sheet) {
		Set<String> unsupportedFunctions = new HashSet<String>();
		Map<CellReference, Exception> unevaluatableCells = new HashMap<CellReference, Exception>();
		for (Row r : sheet) {
			for (Cell c : r) {
				try {
					evaluator.evaluate(c);
				} catch (Exception e) {
					if ((e instanceof NotImplementedException) && ((e.getCause()) != null)) {
						e = ((Exception) (e.getCause()));
					}
					if (e instanceof NotImplementedFunctionException) {
						NotImplementedFunctionException nie = ((NotImplementedFunctionException) (e));
						unsupportedFunctions.add(nie.getFunctionName());
					}
					unevaluatableCells.put(new CellReference(c), e);
				}
			}
		}
		return new CheckFunctionsSupported.FormulaEvaluationProblems(unsupportedFunctions, unevaluatableCells);
	}

	public static class FormulaEvaluationProblems {
		public Set<String> unsupportedFunctions;

		public Map<CellReference, Exception> unevaluatableCells;

		protected FormulaEvaluationProblems(Set<String> unsupportedFunctions, Map<CellReference, Exception> unevaluatableCells) {
			this.unsupportedFunctions = Collections.unmodifiableSet(unsupportedFunctions);
			this.unevaluatableCells = Collections.unmodifiableMap(unevaluatableCells);
		}
	}
}

