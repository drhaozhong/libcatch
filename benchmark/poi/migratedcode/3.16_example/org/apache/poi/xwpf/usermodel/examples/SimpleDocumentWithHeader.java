package org.apache.poi.xwpf.usermodel.examples;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.xwpf.model.XWPFHeaderFooterPolicy;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFFooter;
import org.apache.poi.xwpf.usermodel.XWPFHeader;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.xmlbeans.XmlAnySimpleType;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTP;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTR;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTText;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.STHdrFtr;

import static org.openxmlformats.schemas.wordprocessingml.x2006.main.CTP.Factory.newInstance;


public class SimpleDocumentWithHeader {
	private static XWPFParagraph[] pars;

	public static void main(String[] args) {
		XWPFDocument doc = new XWPFDocument();
		XWPFParagraph p = doc.createParagraph();
		XWPFRun r = p.createRun();
		r.setText("Some Text");
		r.setBold(true);
		r = p.createRun();
		r.setText("Goodbye");
		CTP ctP = newInstance();
		CTText t = ctP.addNewR().addNewT();
		t.setStringValue("header");
		SimpleDocumentWithHeader.pars = new XWPFParagraph[1];
		p = new XWPFParagraph(ctP, doc);
		SimpleDocumentWithHeader.pars[0] = p;
		XWPFHeaderFooterPolicy hfPolicy = doc.createHeaderFooterPolicy();
		hfPolicy.createHeader(XWPFHeaderFooterPolicy.DEFAULT, SimpleDocumentWithHeader.pars);
		ctP = newInstance();
		t = ctP.addNewR().addNewT();
		t.setStringValue("My Footer");
		SimpleDocumentWithHeader.pars[0] = new XWPFParagraph(ctP, doc);
		hfPolicy.createFooter(XWPFHeaderFooterPolicy.DEFAULT, SimpleDocumentWithHeader.pars);
		try {
			OutputStream os = new FileOutputStream(new File("header.docx"));
			doc.write(os);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

