package org.apache.poi.xssf.eventusermodel.examples;


import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.poi.ooxml.util.SAXHelper;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTRst;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;


public class FromHowTo {
	public void processFirstSheet(String filename) throws Exception {
		try (OPCPackage pkg = OPCPackage.open(filename, PackageAccess.READ)) {
			XSSFReader r = new XSSFReader(pkg);
			SharedStringsTable sst = r.getSharedStringsTable();
			XMLReader parser = fetchSheetParser(sst);
			try (InputStream sheet = r.getSheetsData().next()) {
				InputSource sheetSource = new InputSource(sheet);
				parser.parse(sheetSource);
			}
		}
	}

	public void processAllSheets(String filename) throws Exception {
		try (OPCPackage pkg = OPCPackage.open(filename, PackageAccess.READ)) {
			XSSFReader r = new XSSFReader(pkg);
			SharedStringsTable sst = r.getSharedStringsTable();
			XMLReader parser = fetchSheetParser(sst);
			Iterator<InputStream> sheets = r.getSheetsData();
			while (sheets.hasNext()) {
				System.out.println("Processing new sheet:\n");
				try (InputStream sheet = sheets.next()) {
					InputSource sheetSource = new InputSource(sheet);
					parser.parse(sheetSource);
				}
				System.out.println("");
			} 
		}
	}

	public XMLReader fetchSheetParser(SharedStringsTable sst) throws ParserConfigurationException, SAXException {
		XMLReader parser = SAXHelper.newXMLReader();
		ContentHandler handler = new FromHowTo.SheetHandler(sst);
		parser.setContentHandler(handler);
		return parser;
	}

	private static class SheetHandler extends DefaultHandler {
		private final SharedStringsTable sst;

		private String lastContents;

		private boolean nextIsString;

		private boolean inlineStr;

		private final FromHowTo.SheetHandler.LruCache<Integer, String> lruCache = new FromHowTo.SheetHandler.LruCache<>(50);

		private static class LruCache<A, B> extends LinkedHashMap<A, B> {
			private final int maxEntries;

			public LruCache(final int maxEntries) {
				super((maxEntries + 1), 1.0F, true);
				this.maxEntries = maxEntries;
			}

			@Override
			protected boolean removeEldestEntry(final Map.Entry<A, B> eldest) {
				return (super.size()) > (maxEntries);
			}
		}

		private SheetHandler(SharedStringsTable sst) {
			this.sst = sst;
		}

		@Override
		public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
			if (name.equals("c")) {
				System.out.print(((attributes.getValue("r")) + " - "));
				String cellType = attributes.getValue("t");
				nextIsString = (cellType != null) && (cellType.equals("s"));
				inlineStr = (cellType != null) && (cellType.equals("inlineStr"));
			}
			lastContents = "";
		}

		@Override
		public void endElement(String uri, String localName, String name) throws SAXException {
			if (nextIsString) {
				Integer idx = Integer.valueOf(lastContents);
				lastContents = lruCache.get(idx);
				if (((lastContents) == null) && (!(lruCache.containsKey(idx)))) {
					lastContents = new XSSFRichTextString(sst.getEntryAt(idx)).toString();
					lruCache.put(idx, lastContents);
				}
				nextIsString = false;
			}
			if ((name.equals("v")) || ((inlineStr) && (name.equals("c")))) {
				System.out.println(lastContents);
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			lastContents += new String(ch, start, length);
		}
	}

	public static void main(String[] args) throws Exception {
		FromHowTo howto = new FromHowTo();
		howto.processFirstSheet(args[0]);
		howto.processAllSheets(args[0]);
	}
}

