package org.apache.poi.hslf.examples;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import org.apache.poi.hslf.usermodel.HSLFHyperlink;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTextBox;
import org.apache.poi.hslf.usermodel.HSLFTextRun;
import org.apache.poi.hslf.usermodel.HSLFTextShape;


public final class CreateHyperlink {
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		HSLFSlideShow ppt = new HSLFSlideShow();
		HSLFSlide slideA = ppt.createSlide();
		HSLFSlide slideB = ppt.createSlide();
		HSLFSlide slideC = ppt.createSlide();
		HSLFTextBox textBox1 = new HSLFTextBox();
		textBox1.setText("Apache POI");
		textBox1.setAnchor(new Rectangle(100, 100, 200, 50));
		String text = textBox1.getText();
		HSLFHyperlink link = new HSLFHyperlink();
		link.setAddress("http://www.apache.org");
		int linkId = addHyperlink(link);
		slideA.addShape(textBox1);
		HSLFTextBox textBox2 = new HSLFTextBox();
		textBox2.setText("Go to slide #3");
		textBox2.setAnchor(new Rectangle(100, 300, 200, 50));
		HSLFHyperlink link2 = new HSLFHyperlink();
		slideA.addShape(textBox2);
		FileOutputStream out = new FileOutputStream("hyperlink.ppt");
		ppt.write(out);
		out.close();
	}
}

