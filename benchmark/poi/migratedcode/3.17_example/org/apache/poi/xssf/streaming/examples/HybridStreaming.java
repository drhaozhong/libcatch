package org.apache.poi.xssf.streaming.examples;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTSheet;
import org.xml.sax.SAXException;


public class HybridStreaming {
	private static final String SHEET_TO_STREAM = "large sheet";

	public static void main(String[] args) throws IOException, SAXException {
		InputStream sourceBytes = new FileInputStream("workbook.xlsx");
		XSSFWorkbook workbook = new XSSFWorkbook(sourceBytes) {
			@Override
			public void parseSheet(Map<String, XSSFSheet> shIdMap, CTSheet ctSheet) {
				if (!(HybridStreaming.SHEET_TO_STREAM.equals(ctSheet.getName()))) {
					super.parseSheet(shIdMap, ctSheet);
				}
			}
		};
		ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(workbook.getPackage());
		new XSSFSheetXMLHandler(workbook.getStylesSource(), strings, HybridStreaming.createSheetContentsHandler(), false);
		workbook.close();
		sourceBytes.close();
	}

	private static XSSFSheetXMLHandler.SheetContentsHandler createSheetContentsHandler() {
		return new XSSFSheetXMLHandler.SheetContentsHandler() {
			@Override
			public void startRow(int rowNum) {
			}

			@Override
			public void headerFooter(String text, boolean isHeader, String tagName) {
			}

			@Override
			public void endRow(int rowNum) {
			}

			@Override
			public void cell(String cellReference, String formattedValue, XSSFComment comment) {
			}
		};
	}
}

