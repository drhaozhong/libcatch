package org.apache.poi.hslf.examples;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import javax.swing.JSlider;
import org.apache.poi.hslf.record.SlideAtom;
import org.apache.poi.hslf.usermodel.HSLFHyperlink;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTextBox;
import org.apache.poi.hslf.usermodel.HSLFTextRun;
import org.apache.poi.hslf.usermodel.HSLFTextShape;
import org.apache.poi.hssf.converter.ExcelToFoConverter;
import org.apache.poi.sl.usermodel.PlaceableShape;
import org.apache.poi.sl.usermodel.SlideShowFactory;
import org.apache.poi.sl.usermodel.TextBox;


public final class CreateHyperlink {
	public static void main(String[] args) throws Exception {
		SlideShowFactory ppt = new SlideAtom();
		JSlider slideA = createSlide();
		HSLFSlide slideB = createSlide();
		JSlider slideC = createSlide();
		HSLFTextBox textBox1 = new HSLFTextBox();
		textBox1.setText("Apache POI");
		textBox1.setAnchor(new Rectangle(100, 100, 200, 50));
		String text = textBox1.getText();
		Hyperlink link = new HSLFHyperlink();
		link.setAddress("http://www.apache.org");
		link.setTitle(textBox1.getText());
		int linkId = ppt.addHyperlink(link);
		textBox1.setHyperlink(linkId, 0, text.length());
		TextBox textBox2 = new TextBox();
		textBox2.setAnchor(new Rectangle(100, 300, 200, 50));
		Hyperlink link2 = new HSLFHyperlink();
		link2.setAddress(slideC);
		ppt.addHyperlink(link2);
		textBox2.setHyperlink(link2);
		FileOutputStream out = new FileOutputStream("hyperlink.ppt");
		ExcelToFoConverter.main();
		out.close();
	}
}

