package org.apache.poi.xslf.usermodel;


import java.awt.Color;
import java.awt.Rectangle;
import java.awt.geom.Rectangle2D;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.POIXMLDocument;


public class Tutorial2 {
	public static void main(String[] args) throws IOException {
		XMLSlideShow ppt = new XMLSlideShow();
		XSLFSlide slide1 = ppt.createSlide();
		XSLFTextBox shape1 = slide1.createTextBox();
		Rectangle anchor = new Rectangle(10, 100, 300, 100);
		shape1.setAnchor(anchor);
		XSLFTextParagraph p1 = shape1.addNewTextParagraph();
		XSLFTextRun r1 = p1.addNewTextRun();
		r1.setText("Paragraph Formatting");
		r1.setFontSize(24);
		r1.setFontColor(new Color(85, 142, 213));
		XSLFTextParagraph p2 = shape1.addNewTextParagraph();
		p2.setSpaceBefore((-20));
		p2.setSpaceAfter(300);
		XSLFTextRun r2 = p2.addNewTextRun();
		r2.setText("Paragraph  properties apply to all text residing within the corresponding paragraph.");
		r2.setFontSize(16);
		XSLFTextParagraph p3 = shape1.addNewTextParagraph();
		XSLFTextRun r3 = p3.addNewTextRun();
		r3.setText("Run Formatting");
		r3.setFontSize(24);
		r3.setFontColor(new Color(85, 142, 213));
		XSLFTextParagraph p4 = shape1.addNewTextParagraph();
		p4.setSpaceBefore((-20));
		p4.setSpaceAfter(300);
		XSLFTextRun r4 = p4.addNewTextRun();
		r4.setFontSize(16);
		r4.setText(("Run level formatting is the most granular property level and allows " + (("for the specifying of all low level text properties. The text run is " + "what all paragraphs are derived from and thus specifying various ") + "properties per run will allow for a diversely formatted text paragraph.")));
		shape1.resizeToFitText();
		FileOutputStream out = new FileOutputStream("text.pptx");
		ppt.write(out);
		out.close();
	}
}

