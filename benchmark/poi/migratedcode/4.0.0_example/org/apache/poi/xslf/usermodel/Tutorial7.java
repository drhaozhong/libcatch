package org.apache.poi.xslf.usermodel;


import java.awt.Color;
import java.awt.Rectangle;
import java.awt.geom.Rectangle2D;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.sl.usermodel.AutoNumberingScheme;


public class Tutorial7 {
	public static void main(String[] args) throws IOException {
		try (XMLSlideShow ppt = new XMLSlideShow()) {
			XSLFSlide slide = ppt.createSlide();
			XSLFTextBox shape = slide.createTextBox();
			shape.setAnchor(new Rectangle(50, 50, 400, 200));
			XSLFTextParagraph p1 = shape.addNewTextParagraph();
			p1.setIndentLevel(0);
			p1.setBullet(true);
			XSLFTextRun r1 = p1.addNewTextRun();
			r1.setText("Bullet1");
			XSLFTextParagraph p2 = shape.addNewTextParagraph();
			p2.setLeftMargin(60.0);
			p2.setIndent((-40.0));
			p2.setBullet(true);
			p2.setBulletFontColor(Color.red);
			p2.setBulletFont("Wingdings");
			p2.setBulletCharacter("u");
			p2.setIndentLevel(1);
			XSLFTextRun r2 = p2.addNewTextRun();
			r2.setText("Bullet2");
			XSLFTextParagraph p3 = shape.addNewTextParagraph();
			p3.setBulletAutoNumber(AutoNumberingScheme.alphaLcParenRight, 1);
			p3.setIndentLevel(2);
			XSLFTextRun r3 = p3.addNewTextRun();
			r3.setText("Numbered List Item - 1");
			XSLFTextParagraph p4 = shape.addNewTextParagraph();
			p4.setBulletAutoNumber(AutoNumberingScheme.alphaLcParenRight, 2);
			p4.setIndentLevel(2);
			XSLFTextRun r4 = p4.addNewTextRun();
			r4.setText("Numbered List Item - 2");
			XSLFTextParagraph p5 = shape.addNewTextParagraph();
			p5.setBulletAutoNumber(AutoNumberingScheme.alphaLcParenRight, 3);
			p5.setIndentLevel(2);
			XSLFTextRun r5 = p5.addNewTextRun();
			r5.setText("Numbered List Item - 3");
			shape.resizeToFitText();
			try (FileOutputStream out = new FileOutputStream("list.pptx")) {
				ppt.write(out);
			}
		}
	}
}

