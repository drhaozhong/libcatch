package org.apache.poi.hslf.examples;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.poi.hslf.usermodel.HSLFHyperlink;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTextBox;
import org.apache.poi.hslf.usermodel.HSLFTextParagraph;
import org.apache.poi.hslf.usermodel.HSLFTextRun;
import org.apache.poi.hslf.usermodel.HSLFTextShape;


public abstract class CreateHyperlink {
	public static void main(String[] args) throws IOException {
		HSLFSlideShow ppt = new HSLFSlideShow();
		try {
			HSLFSlide slideA = ppt.createSlide();
			ppt.createSlide();
			HSLFSlide slideC = ppt.createSlide();
			HSLFTextBox textBox1 = slideA.createTextBox();
			textBox1.setText("Apache POI");
			textBox1.setAnchor(new Rectangle(100, 100, 200, 50));
			HSLFHyperlink link1 = textBox1.getTextParagraphs().get(0).getTextRuns().get(0).createHyperlink();
			link1.linkToUrl("http://www.apache.org");
			link1.setLabel(textBox1.getText());
			HSLFTextBox textBox2 = slideA.createTextBox();
			textBox2.setText("Go to slide #3");
			textBox2.setAnchor(new Rectangle(100, 300, 200, 50));
			HSLFHyperlink link2 = textBox2.getTextParagraphs().get(0).getTextRuns().get(0).createHyperlink();
			link2.linkToSlide(slideC);
			FileOutputStream out = new FileOutputStream("hyperlink.ppt");
			ppt.write(out);
			out.close();
		} finally {
			ppt.close();
		}
	}
}

