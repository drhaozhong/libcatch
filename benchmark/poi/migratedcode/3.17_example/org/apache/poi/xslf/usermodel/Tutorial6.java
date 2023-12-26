package org.apache.poi.xslf.usermodel;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ooxml.POIXMLDocument;


public class Tutorial6 {
	public static void main(String[] args) throws IOException {
		XMLSlideShow ppt = new XMLSlideShow();
		try {
			XSLFSlide slide1 = ppt.createSlide();
			XSLFSlide slide2 = ppt.createSlide();
			XSLFTextBox shape1 = slide1.createTextBox();
			shape1.setAnchor(new Rectangle(50, 50, 200, 50));
			XSLFTextRun r1 = shape1.addNewTextParagraph().addNewTextRun();
			XSLFHyperlink link1 = r1.createHyperlink();
			r1.setText("http://poi.apache.org");
			link1.setAddress("http://poi.apache.org");
			XSLFTextBox shape2 = slide1.createTextBox();
			shape2.setAnchor(new Rectangle(300, 50, 200, 50));
			XSLFTextRun r2 = shape2.addNewTextParagraph().addNewTextRun();
			XSLFHyperlink link2 = r2.createHyperlink();
			r2.setText("Go to the second slide");
			link2.linkToSlide(slide2);
			FileOutputStream out = new FileOutputStream("hyperlinks.pptx");
			ppt.write(out);
			out.close();
		} finally {
			ppt.close();
		}
	}
}

