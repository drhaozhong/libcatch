package org.apache.poi.hslf.examples;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTextBox;
import org.apache.poi.hslf.usermodel.HSLFTextParagraph;
import org.apache.poi.hslf.usermodel.HSLFTextRun;
import org.apache.poi.hslf.usermodel.HSLFTextShape;


public final class BulletsDemo {
	public static void main(String[] args) throws IOException {
		HSLFSlideShow ppt = new HSLFSlideShow();
		try {
			HSLFSlide slide = ppt.createSlide();
			HSLFTextBox shape = new HSLFTextBox();
			HSLFTextParagraph rt = shape.getTextParagraphs().get(0);
			rt.getTextRuns().get(0).setFontSize(42.0);
			rt.setBullet(true);
			rt.setIndent(0.0);
			rt.setLeftMargin(50.0);
			rt.setBulletChar('\u263a');
			shape.setText(("January\r" + (("February\r" + "March\r") + "April")));
			slide.addShape(shape);
			shape.setAnchor(new Rectangle(50, 50, 500, 300));
			slide.addShape(shape);
			FileOutputStream out = new FileOutputStream("bullets.ppt");
			ppt.write(out);
			out.close();
		} finally {
			ppt.close();
		}
	}
}

