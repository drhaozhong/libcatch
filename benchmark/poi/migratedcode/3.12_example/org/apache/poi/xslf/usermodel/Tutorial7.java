package org.apache.poi.xslf.usermodel;


import java.awt.Color;
import java.awt.Rectangle;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.POIXMLDocument;


public class Tutorial7 {
	public static void main(String[] args) throws IOException {
		XMLSlideShow ppt = new XMLSlideShow();
		XSLFSlide slide = ppt.createSlide();
		XSLFTextBox shape = slide.createTextBox();
		shape.setAnchor(new Rectangle(50, 50, 400, 200));
		XSLFTextParagraph p1 = shape.addNewTextParagraph();
		p1.setBullet(true);
		XSLFTextRun r1 = p1.addNewTextRun();
		r1.setText("Bullet1");
		XSLFTextParagraph p2 = shape.addNewTextParagraph();
		p2.setBullet(true);
		p2.setBulletFontColor(Color.red);
		p2.setBulletFont("Wingdings");
		p2.setBulletCharacter("u");
		XSLFTextRun r2 = p2.addNewTextRun();
		r2.setText("Bullet2");
		XSLFTextParagraph p3 = shape.addNewTextParagraph();
		XSLFTextRun r3 = p3.addNewTextRun();
		r3.setText("Numbered List Item - 1");
		XSLFTextParagraph p4 = shape.addNewTextParagraph();
		XSLFTextRun r4 = p4.addNewTextRun();
		r4.setText("Numbered List Item - 2");
		XSLFTextParagraph p5 = shape.addNewTextParagraph();
		XSLFTextRun r5 = p5.addNewTextRun();
		r5.setText("Numbered List Item - 3");
		shape.resizeToFitText();
		FileOutputStream out = new FileOutputStream("list.pptx");
		ppt.write(out);
		out.close();
	}
}

