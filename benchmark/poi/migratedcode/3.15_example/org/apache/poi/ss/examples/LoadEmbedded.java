package org.apache.poi.ss.examples;


import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hssf.usermodel.HSSFObjectData;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.sl.usermodel.SlideShow;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xslf.usermodel.XSLFSlideShow;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.usermodel.XWPFDocument;


public class LoadEmbedded {
	public static void main(String[] args) throws Exception {
		Workbook wb = WorkbookFactory.create(new File(args[0]));
		LoadEmbedded.loadEmbedded(wb);
	}

	public static void loadEmbedded(Workbook wb) throws Exception {
		if (wb instanceof HSSFWorkbook) {
			LoadEmbedded.loadEmbedded(((HSSFWorkbook) (wb)));
		}else
			if (wb instanceof XSSFWorkbook) {
				LoadEmbedded.loadEmbedded(((XSSFWorkbook) (wb)));
			}else {
				throw new IllegalArgumentException(wb.getClass().getName());
			}

	}

	public static void loadEmbedded(HSSFWorkbook workbook) throws Exception {
		for (HSSFObjectData obj : workbook.getAllEmbeddedObjects()) {
			String oleName = obj.getOLE2ClassName();
			if (oleName.equals("Worksheet")) {
				DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
				HSSFWorkbook embeddedWorkbook = new HSSFWorkbook(dn, false);
			}else
				if (oleName.equals("Document")) {
					DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
					HWPFDocument embeddedWordDocument = new HWPFDocument(dn);
				}else
					if (oleName.equals("Presentation")) {
						DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
						SlideShow<?, ?> embeddedPowerPointDocument = new HSLFSlideShow(dn);
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

	public static void loadEmbedded(XSSFWorkbook workbook) throws Exception {
		for (PackagePart pPart : workbook.getAllEmbedds()) {
			String contentType = pPart.getContentType();
			if (contentType.equals("application/vnd.ms-excel")) {
				HSSFWorkbook embeddedWorkbook = new HSSFWorkbook(pPart.getInputStream());
			}else
				if (contentType.equals("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")) {
					OPCPackage docPackage = OPCPackage.open(pPart.getInputStream());
					XSSFWorkbook embeddedWorkbook = new XSSFWorkbook(docPackage);
				}else
					if (contentType.equals("application/msword")) {
						HWPFDocument document = new HWPFDocument(pPart.getInputStream());
					}else
						if (contentType.equals("application/vnd.openxmlformats-officedocument.wordprocessingml.document")) {
							OPCPackage docPackage = OPCPackage.open(pPart.getInputStream());
							XWPFDocument document = new XWPFDocument(docPackage);
						}else
							if (contentType.equals("application/vnd.ms-powerpoint")) {
								HSLFSlideShow slideShow = new HSLFSlideShow(pPart.getInputStream());
							}else
								if (contentType.equals("application/vnd.openxmlformats-officedocument.presentationml.presentation")) {
									OPCPackage docPackage = OPCPackage.open(pPart.getInputStream());
									XSLFSlideShow slideShow = new XSLFSlideShow(docPackage);
								}else {
									System.out.println(("Unknown Embedded Document: " + contentType));
									InputStream inputStream = pPart.getInputStream();
								}





		}
	}
}

