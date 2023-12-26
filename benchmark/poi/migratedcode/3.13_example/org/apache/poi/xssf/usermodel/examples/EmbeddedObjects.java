package org.apache.poi.xssf.usermodel.examples;


import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.hslf.usermodel.HSLFSlideShowImpl;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.usermodel.XWPFDocument;


public class EmbeddedObjects {
	public static void main(String[] args) throws Exception {
		OPCPackage pkg = OPCPackage.open(args[0]);
		XSSFWorkbook workbook = new XSSFWorkbook(pkg);
		for (PackagePart pPart : workbook.getAllEmbedds()) {
			String contentType = pPart.getContentType();
			if (contentType.equals("application/vnd.ms-excel")) {
				HSSFWorkbook embeddedWorkbook = new HSSFWorkbook(pPart.getInputStream());
			}else
				if (contentType.equals("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")) {
					XSSFWorkbook embeddedWorkbook = new XSSFWorkbook(pPart.getInputStream());
				}else
					if (contentType.equals("application/msword")) {
						HWPFDocument document = new HWPFDocument(pPart.getInputStream());
					}else
						if (contentType.equals("application/vnd.openxmlformats-officedocument.wordprocessingml.document")) {
							XWPFDocument document = new XWPFDocument(pPart.getInputStream());
						}else
							if (contentType.equals("application/vnd.ms-powerpoint")) {
								HSLFSlideShowImpl slideShow = new HSLFSlideShowImpl(pPart.getInputStream());
							}else
								if (contentType.equals("application/vnd.openxmlformats-officedocument.presentationml.presentation")) {
									OPCPackage docPackage = OPCPackage.open(pPart.getInputStream());
									XMLSlideShow slideShow = new XMLSlideShow(docPackage);
								}else {
									System.out.println(("Unknown Embedded Document: " + contentType));
									InputStream inputStream = pPart.getInputStream();
								}





		}
		pkg.close();
	}
}

