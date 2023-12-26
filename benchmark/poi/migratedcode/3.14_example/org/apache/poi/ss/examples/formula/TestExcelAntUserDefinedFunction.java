package org.apache.poi.ss.examples.formula;


import junit.framework.TestCase;
import org.apache.poi.ss.excelant.ExcelAntUserDefinedFunction;


public class TestExcelAntUserDefinedFunction extends TestCase {
	private ExcelAntUserDefinedFunctionTestHelper fixture;

	@Override
	public void setUp() {
		fixture = new ExcelAntUserDefinedFunctionTestHelper();
	}

	public void testSetClassName() {
		String className = "simple.class.name";
		fixture.setClassName(className);
		String value = fixture.getClassName();
		TestCase.assertNotNull(value);
		TestCase.assertEquals(className, value);
	}

	public void testSetFunction() {
		String functionAlias = "alias";
		fixture.setFunctionAlias(functionAlias);
		String alias = fixture.getFunctionAlias();
		TestCase.assertNotNull(alias);
		TestCase.assertEquals(functionAlias, alias);
	}
}

