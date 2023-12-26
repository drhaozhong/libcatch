package org.apache.poi.ss.examples;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.POIDocument;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hssf.usermodel.HSSFObjectData;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.sl.usermodel.SlideShow;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.xmlbeans.XmlException;


public class LoadEmbedded {
	public static void main(String[] args) throws IOException, EncryptedDocumentException, OpenXML4JException, XmlException {
		Workbook wb = WorkbookFactory.create(new File(args[0]));
		LoadEmbedded.loadEmbedded(wb);
	}

	public static void loadEmbedded(Workbook wb) throws IOException, InvalidFormatException, OpenXML4JException, XmlException {
		if (wb instanceof HSSFWorkbook) {
			LoadEmbedded.loadEmbedded(((HSSFWorkbook) (wb)));
		}else
			if (wb instanceof XSSFWorkbook) {
				LoadEmbedded.loadEmbedded(((XSSFWorkbook) (wb)));
			}else {
				throw new IllegalArgumentException(wb.getClass().getName());
			}

	}

	public static void loadEmbedded(HSSFWorkbook workbook) throws IOException {
		for (HSSFObjectData obj : workbook.getAllEmbeddedObjects()) {
			String oleName = obj.getOLE2ClassName();
			if (oleName.equals("Worksheet")) {
				DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
				HSSFWorkbook embeddedWorkbook = new HSSFWorkbook(dn, false);
				embeddedWorkbook.close();
			}else
				if (oleName.equals("Document")) {
					DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
					HWPFDocument embeddedWordDocument = new HWPFDocument(dn);
					embeddedWordDocument.close();
				}else
					if (oleName.equals("Presentation")) {
						DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
						SlideShow<?, ?> embeddedSlieShow = new HSLFSlideShow(dn);
						embeddedSlieShow.close();
					}else {
						if (obj.hasDirectoryEntry()) {
							DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
							for (Entry entry : dn) {
							}
						}else {
							byte[] objectData = obj.getObjectData();
						}
					}


		}
	}

	public static void loadEmbedded(XSSFWorkbook workbook) throws IOException, InvalidFormatException, OpenXML4JException, XmlException {
		for (PackagePart pPart : workbook.getAllEmbedds()) {
			String contentType = pPart.getContentType();
			if (contentType.equals("application/vnd.ms-excel")) {
				HSSFWorkbook embeddedWorkbook = new HSSFWorkbook(pPart.getInputStream());
				embeddedWorkbook.close();
			}else
				if (contentType.equals("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")) {
					XSSFWorkbook embeddedWorkbook = new XSSFWorkbook(pPart.getInputStream());
					embeddedWorkbook.close();
				}else
					if (contentType.equals("application/msword")) {
						HWPFDocument document = new HWPFDocument(pPart.getInputStream());
						document.close();
					}else
						if (contentType.equals("application/vnd.openxmlformats-officedocument.wordprocessingml.document")) {
							XWPFDocument document = new XWPFDocument(pPart.getInputStream());
							document.close();
						}else
							if (contentType.equals("application/vnd.ms-powerpoint")) {
								HSLFSlideShow slideShow = new HSLFSlideShow(pPart.getInputStream());
								slideShow.close();
							}else
								if (contentType.equals("application/vnd.openxmlformats-officedocument.presentationml.presentation")) {
									XMLSlideShow slideShow = new XMLSlideShow(pPart.getInputStream());
									slideShow.close();
								}else {
									System.out.println(("Unknown Embedded Document: " + contentType));
									InputStream inputStream = pPart.getInputStream();
									inputStream.close();
								}





		}
	}
}

