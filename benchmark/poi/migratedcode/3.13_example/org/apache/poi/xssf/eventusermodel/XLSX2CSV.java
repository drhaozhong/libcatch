package org.apache.poi.xssf.eventusermodel;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Iterator;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;


public class XLSX2CSV {
	enum xssfDataType {

		BOOL,
		ERROR,
		FORMULA,
		INLINESTR,
		SSTINDEX,
		NUMBER;}

	class MyXSSFSheetHandler extends DefaultHandler {
		private StylesTable stylesTable;

		private ReadOnlySharedStringsTable sharedStringsTable;

		private boolean vIsOpen;

		private XLSX2CSV.xssfDataType nextDataType;

		private short formatIndex;

		private String formatString;

		private final DataFormatter formatter;

		private int thisColumn = -1;

		private int lastColumnNumber = -1;

		private StringBuffer value;

		public MyXSSFSheetHandler(StylesTable styles, ReadOnlySharedStringsTable strings) {
			this.stylesTable = styles;
			this.sharedStringsTable = strings;
			this.value = new StringBuffer();
			this.nextDataType = XLSX2CSV.xssfDataType.NUMBER;
			this.formatter = new DataFormatter();
		}

		public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
			if (("inlineStr".equals(name)) || ("v".equals(name))) {
				vIsOpen = true;
				value.setLength(0);
			}else
				if ("c".equals(name)) {
					String r = attributes.getValue("r");
					int firstDigit = -1;
					for (int c = 0; c < (r.length()); ++c) {
						if (Character.isDigit(r.charAt(c))) {
							firstDigit = c;
							break;
						}
					}
					thisColumn = nameToColumn(r.substring(0, firstDigit));
					this.nextDataType = XLSX2CSV.xssfDataType.NUMBER;
					this.formatIndex = -1;
					this.formatString = null;
					String cellType = attributes.getValue("t");
					String cellStyleStr = attributes.getValue("s");
					if ("b".equals(cellType))
						nextDataType = XLSX2CSV.xssfDataType.BOOL;
					else
						if ("e".equals(cellType))
							nextDataType = XLSX2CSV.xssfDataType.ERROR;
						else
							if ("inlineStr".equals(cellType))
								nextDataType = XLSX2CSV.xssfDataType.INLINESTR;
							else
								if ("s".equals(cellType))
									nextDataType = XLSX2CSV.xssfDataType.SSTINDEX;
								else
									if ("str".equals(cellType))
										nextDataType = XLSX2CSV.xssfDataType.FORMULA;
									else
										if (cellStyleStr != null) {
											XSSFCellStyle style = null;
											if (cellStyleStr != null) {
												int styleIndex = Integer.parseInt(cellStyleStr);
												style = stylesTable.getStyleAt(styleIndex);
											}
											if ((style == null) && ((stylesTable.getNumCellStyles()) > 0)) {
												style = stylesTable.getStyleAt(0);
											}
											if (style != null) {
												this.formatIndex = style.getDataFormat();
												this.formatString = style.getDataFormatString();
												if ((this.formatString) == null)
													this.formatString = BuiltinFormats.getBuiltinFormat(this.formatIndex);

											}
										}





				}

		}

		public void endElement(String uri, String localName, String name) throws SAXException {
			String thisStr = null;
			if ("v".equals(name)) {
				switch (nextDataType) {
					case BOOL :
						char first = value.charAt(0);
						thisStr = (first == '0') ? "FALSE" : "TRUE";
						break;
					case ERROR :
						thisStr = ("\"ERROR:" + (value.toString())) + '"';
						break;
					case FORMULA :
						thisStr = ('"' + (value.toString())) + '"';
						break;
					case INLINESTR :
						XSSFRichTextString rtsi = new XSSFRichTextString(value.toString());
						thisStr = ('"' + (rtsi.toString())) + '"';
						break;
					case SSTINDEX :
						String sstIndex = value.toString();
						try {
							int idx = Integer.parseInt(sstIndex);
							XSSFRichTextString rtss = new XSSFRichTextString(sharedStringsTable.getEntryAt(idx));
							thisStr = ('"' + (rtss.toString())) + '"';
						} catch (NumberFormatException ex) {
							output.println(((("Failed to parse SST index '" + sstIndex) + "': ") + (ex.toString())));
						}
						break;
					case NUMBER :
						String n = value.toString();
						if (((this.formatString) != null) && ((n.length()) > 0))
							thisStr = formatter.formatRawCellContents(Double.parseDouble(n), this.formatIndex, this.formatString);
						else
							thisStr = n;

						break;
					default :
						thisStr = ("(TODO: Unexpected type: " + (nextDataType)) + ")";
						break;
				}
				if ((lastColumnNumber) == (-1)) {
					lastColumnNumber = 0;
				}
				for (int i = lastColumnNumber; i < (thisColumn); ++i)
					output.print(',');

				output.print(thisStr);
				if ((thisColumn) > (-1))
					lastColumnNumber = thisColumn;

			}else
				if ("row".equals(name)) {
					if ((minColumns) > 0) {
						if ((lastColumnNumber) == (-1)) {
							lastColumnNumber = 0;
						}
						for (int i = lastColumnNumber; i < (minColumns); i++) {
							output.print(',');
						}
					}
					output.println();
					lastColumnNumber = -1;
				}

		}

		public void characters(char[] ch, int start, int length) throws SAXException {
			if (vIsOpen)
				value.append(ch, start, length);

		}

		private int nameToColumn(String name) {
			int column = -1;
			for (int i = 0; i < (name.length()); ++i) {
				int c = name.charAt(i);
				column = (((column + 1) * 26) + c) - 'A';
			}
			return column;
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

	public void processSheet(StylesTable styles, ReadOnlySharedStringsTable strings, InputStream sheetInputStream) throws IOException, ParserConfigurationException, SAXException {
		InputSource sheetSource = new InputSource(sheetInputStream);
		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		SAXParser saxParser = saxFactory.newSAXParser();
		XMLReader sheetParser = saxParser.getXMLReader();
		ContentHandler handler = new XLSX2CSV.MyXSSFSheetHandler(styles, strings);
		sheetParser.setContentHandler(handler);
		sheetParser.parse(sheetSource);
	}

	public void process() throws IOException, ParserConfigurationException, OpenXML4JException, SAXException {
		ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(this.xlsxPackage);
		XSSFReader xssfReader = new XSSFReader(this.xlsxPackage);
		StylesTable styles = xssfReader.getStylesTable();
		XSSFReader.SheetIterator iter = ((XSSFReader.SheetIterator) (xssfReader.getSheetsData()));
		int index = 0;
		while (iter.hasNext()) {
			InputStream stream = iter.next();
			String sheetName = iter.getSheetName();
			this.output.println();
			this.output.println((((sheetName + " [index=") + index) + "]:"));
			processSheet(styles, strings, stream);
			stream.close();
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

		OPCPackage p = OPCPackage.open(xlsxFile.getPath(), PackageAccess.READ);
		XLSX2CSV xlsx2csv = new XLSX2CSV(p, System.out, minColumns);
		xlsx2csv.process();
		p.close();
	}
}

