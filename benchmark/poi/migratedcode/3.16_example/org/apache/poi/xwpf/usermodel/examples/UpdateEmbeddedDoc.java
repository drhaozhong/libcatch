package org.apache.poi.xwpf.usermodel.examples;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import org.apache.poi.util.IOUtils;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
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
			IOUtils.closeQuietly(fis);
		}
	}

	public void updateEmbeddedDoc() throws IOException, OpenXML4JException {
		List<PackagePart> embeddedDocs = this.doc.getAllEmbedds();
		for (PackagePart pPart : embeddedDocs) {
			String ext = pPart.getPartName().getExtension();
			if ((UpdateEmbeddedDoc.BINARY_EXTENSION.equals(ext)) || (UpdateEmbeddedDoc.OPENXML_EXTENSION.equals(ext))) {
				InputStream is = pPart.getInputStream();
				Workbook workbook = null;
				OutputStream os = null;
				try {
					workbook = WorkbookFactory.create(is);
					Sheet sheet = workbook.getSheetAt(UpdateEmbeddedDoc.SHEET_NUM);
					Row row = sheet.getRow(UpdateEmbeddedDoc.ROW_NUM);
					Cell cell = row.getCell(UpdateEmbeddedDoc.CELL_NUM);
					cell.setCellValue(UpdateEmbeddedDoc.NEW_VALUE);
					os = pPart.getOutputStream();
					workbook.write(os);
				} finally {
					IOUtils.closeQuietly(os);
					IOUtils.closeQuietly(workbook);
					IOUtils.closeQuietly(is);
				}
			}
		}
		if (!(embeddedDocs.isEmpty())) {
			FileOutputStream fos = new FileOutputStream(this.docFile);
			this.doc.write(fos);
			fos.close();
		}
	}

	public void checkUpdatedDoc() throws IOException, OpenXML4JException {
		for (PackagePart pPart : this.doc.getAllEmbedds()) {
			String ext = pPart.getPartName().getExtension();
			if ((UpdateEmbeddedDoc.BINARY_EXTENSION.equals(ext)) || (UpdateEmbeddedDoc.OPENXML_EXTENSION.equals(ext))) {
				InputStream is = pPart.getInputStream();
				Workbook workbook = null;
				try {
					workbook = WorkbookFactory.create(is);
					Sheet sheet = workbook.getSheetAt(UpdateEmbeddedDoc.SHEET_NUM);
					Row row = sheet.getRow(UpdateEmbeddedDoc.ROW_NUM);
					Cell cell = row.getCell(UpdateEmbeddedDoc.CELL_NUM);
					Assert.assertEquals(cell.getNumericCellValue(), UpdateEmbeddedDoc.NEW_VALUE, 1.0E-4);
				} finally {
					IOUtils.closeQuietly(workbook);
					IOUtils.closeQuietly(is);
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, OpenXML4JException {
		UpdateEmbeddedDoc ued = new UpdateEmbeddedDoc(args[0]);
		ued.updateEmbeddedDoc();
		ued.checkUpdatedDoc();
	}
}

