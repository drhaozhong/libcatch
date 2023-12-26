package org.apache.poi.xwpf.usermodel;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.openxml4j.opc.PackagePartName;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.Assert;


public class UpdateEmbeddedDoc {
	private XWPFDocument doc = null;

	private File docFile = null;

	private static final int SHEET_NUM = 0;

	private static final int ROW_NUM = 0;

	private static final int CELL_NUM = 0;

	private static final double NEW_VALUE = 100.98;

	private static final String BINARY_EXTENSION = "xls";

	private static final String OPENXML_EXTENSION = "xlsx";

	public UpdateEmbeddedDoc(String filename) throws FileNotFoundException, IOException {
		this.docFile = new File(filename);
		FileInputStream fis = null;
		if (!(this.docFile.exists())) {
			throw new FileNotFoundException((("The Word dcoument " + filename) + " does not exist."));
		}
		try {
			fis = new FileInputStream(this.docFile);
			this.doc = new XWPFDocument(fis);
		} finally {
			if (fis != null) {
				try {
					fis.close();
					fis = null;
				} catch (IOException ioEx) {
					System.out.println(("IOException caught trying to close " + ("FileInputStream in the constructor of " + "UpdateEmbeddedDoc.")));
				}
			}
		}
	}

	public void updateEmbeddedDoc() throws IOException, OpenXML4JException {
		Workbook workbook = null;
		Sheet sheet = null;
		Row row = null;
		Cell cell = null;
		PackagePart pPart = null;
		Iterator<PackagePart> pIter = null;
		List<PackagePart> embeddedDocs = this.doc.getAllEmbedds();
		if ((embeddedDocs != null) && (!(embeddedDocs.isEmpty()))) {
			pIter = embeddedDocs.iterator();
			while (pIter.hasNext()) {
				pPart = pIter.next();
				if ((pPart.getPartName().getExtension().equals(UpdateEmbeddedDoc.BINARY_EXTENSION)) || (pPart.getPartName().getExtension().equals(UpdateEmbeddedDoc.OPENXML_EXTENSION))) {
					workbook = WorkbookFactory.create(pPart.getInputStream());
					sheet = workbook.getSheetAt(UpdateEmbeddedDoc.SHEET_NUM);
					row = sheet.getRow(UpdateEmbeddedDoc.ROW_NUM);
					cell = row.getCell(UpdateEmbeddedDoc.CELL_NUM);
					cell.setCellValue(UpdateEmbeddedDoc.NEW_VALUE);
					workbook.write(pPart.getOutputStream());
				}
			} 
			this.doc.write(new FileOutputStream(this.docFile));
		}
	}

	public void checkUpdatedDoc() throws IOException, OpenXML4JException {
		Workbook workbook = null;
		Sheet sheet = null;
		Row row = null;
		Cell cell = null;
		PackagePart pPart = null;
		Iterator<PackagePart> pIter = null;
		List<PackagePart> embeddedDocs = this.doc.getAllEmbedds();
		if ((embeddedDocs != null) && (!(embeddedDocs.isEmpty()))) {
			pIter = embeddedDocs.iterator();
			while (pIter.hasNext()) {
				pPart = pIter.next();
				if ((pPart.getPartName().getExtension().equals(UpdateEmbeddedDoc.BINARY_EXTENSION)) || (pPart.getPartName().getExtension().equals(UpdateEmbeddedDoc.OPENXML_EXTENSION))) {
					workbook = WorkbookFactory.create(pPart.getInputStream());
					sheet = workbook.getSheetAt(UpdateEmbeddedDoc.SHEET_NUM);
					row = sheet.getRow(UpdateEmbeddedDoc.ROW_NUM);
					cell = row.getCell(UpdateEmbeddedDoc.CELL_NUM);
					Assert.assertEquals(cell.getNumericCellValue(), UpdateEmbeddedDoc.NEW_VALUE, 1.0E-4);
				}
			} 
		}
	}

	public static void main(String[] args) {
		try {
			UpdateEmbeddedDoc ued = new UpdateEmbeddedDoc(args[0]);
			ued.updateEmbeddedDoc();
			ued.checkUpdatedDoc();
		} catch (Exception ex) {
			System.out.println(ex.getClass().getName());
			System.out.println(ex.getMessage());
			ex.printStackTrace(System.out);
		}
	}
}

