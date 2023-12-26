package org.apache.poi.xssf.eventusermodel;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Iterator;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.poi.ooxml.util.SAXHelper;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.model.Styles;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;


public class XLSX2CSV {
	private class SheetToCSV implements XSSFSheetXMLHandler.SheetContentsHandler {
		private boolean firstCellOfRow;

		private int currentRow = -1;

		private int currentCol = -1;

		private void outputMissingRows(int number) {
			for (int i = 0; i < number; i++) {
				for (int j = 0; j < (minColumns); j++) {
					output.append(',');
				}
				output.append('\n');
			}
		}

		@Override
		public void startRow(int rowNum) {
			outputMissingRows(((rowNum - (currentRow)) - 1));
			firstCellOfRow = true;
			currentRow = rowNum;
			currentCol = -1;
		}

		@Override
		public void endRow(int rowNum) {
			for (int i = currentCol; i < (minColumns); i++) {
				output.append(',');
			}
			output.append('\n');
		}

		@Override
		public void cell(String cellReference, String formattedValue, XSSFComment comment) {
			if (firstCellOfRow) {
				firstCellOfRow = false;
			}else {
				output.append(',');
			}
			if (cellReference == null) {
				cellReference = new CellAddress(currentRow, currentCol).formatAsString();
			}
			int thisCol = new CellReference(cellReference).getCol();
			int missedCols = (thisCol - (currentCol)) - 1;
			for (int i = 0; i < missedCols; i++) {
				output.append(',');
			}
			currentCol = thisCol;
			try {
				Double.parseDouble(formattedValue);
				output.append(formattedValue);
			} catch (NumberFormatException e) {
				output.append('"');
				output.append(formattedValue);
				output.append('"');
			}
		}
	}

	private final OPCPackage xlsxPackage;

	private final int minColumns;

	private final PrintStream output;

	public XLSX2CSV(OPCPackage pkg, PrintStream output, int minColumns) {
		this.xlsxPackage = pkg;
		this.output = output;
		this.minColumns = minColumns;
	}

	public void processSheet(Styles styles, SharedStrings strings, XSSFSheetXMLHandler.SheetContentsHandler sheetHandler, InputStream sheetInputStream) throws IOException, SAXException {
		DataFormatter formatter = new DataFormatter();
		InputSource sheetSource = new InputSource(sheetInputStream);
		try {
			XMLReader sheetParser = SAXHelper.newXMLReader();
			ContentHandler handler = new XSSFSheetXMLHandler(styles, null, strings, sheetHandler, formatter, false);
			sheetParser.setContentHandler(handler);
			sheetParser.parse(sheetSource);
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(("SAX parser appears to be broken - " + (e.getMessage())));
		}
	}

	public void process() throws IOException, OpenXML4JException, SAXException {
		ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(this.xlsxPackage);
		XSSFReader xssfReader = new XSSFReader(this.xlsxPackage);
		StylesTable styles = xssfReader.getStylesTable();
		XSSFReader.SheetIterator iter = ((XSSFReader.SheetIterator) (xssfReader.getSheetsData()));
		int index = 0;
		while (iter.hasNext()) {
			try (InputStream stream = iter.next()) {
				String sheetName = iter.getSheetName();
				this.output.println();
				this.output.println((((sheetName + " [index=") + index) + "]:"));
				processSheet(styles, strings, new XLSX2CSV.SheetToCSV(), stream);
			}
			++index;
		} 
	}

	public static void main(String[] args) throws Exception {
		if ((args.length) < 1) {
			System.err.println("Use:");
			System.err.println("  XLSX2CSV <xlsx file> [min columns]");
			return;
		}
		File xlsxFile = new File(args[0]);
		if (!(xlsxFile.exists())) {
			System.err.println(("Not found or not a file: " + (xlsxFile.getPath())));
			return;
		}
		int minColumns = -1;
		if ((args.length) >= 2)
			minColumns = Integer.parseInt(args[1]);

		try (OPCPackage p = OPCPackage.open(xlsxFile.getPath(), PackageAccess.READ)) {
			XLSX2CSV xlsx2csv = new XLSX2CSV(p, System.out, minColumns);
			xlsx2csv.process();
		}
	}
}

