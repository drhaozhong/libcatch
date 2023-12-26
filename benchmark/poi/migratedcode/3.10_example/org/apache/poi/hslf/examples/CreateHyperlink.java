package org.apache.poi.hslf.examples;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import org.apache.poi.hslf.model.Hyperlink;
import org.apache.poi.hslf.model.Shape;
import org.apache.poi.hslf.model.Sheet;
import org.apache.poi.hslf.model.SimpleShape;
import org.apache.poi.hslf.model.Slide;
import org.apache.poi.hslf.model.TextBox;
import org.apache.poi.hslf.model.TextShape;
import org.apache.poi.hslf.usermodel.SlideShow;


public final class CreateHyperlink {
	public static void main(String[] args) throws Exception {
		SlideShow ppt = new SlideShow();
		Slide slideA = ppt.createSlide();
		Slide slideB = ppt.createSlide();
		Slide slideC = ppt.createSlide();
		TextBox textBox1 = new TextBox();
		textBox1.setText("Apache POI");
		textBox1.setAnchor(new Rectangle(100, 100, 200, 50));
		String text = textBox1.getText();
		Hyperlink link = new Hyperlink();
		link.setAddress("http://www.apache.org");
		link.setTitle(textBox1.getText());
		int linkId = ppt.addHyperlink(link);
		textBox1.setHyperlink(linkId, 0, text.length());
		slideA.addShape(textBox1);
		TextBox textBox2 = new TextBox();
		textBox2.setText("Go to slide #3");
		textBox2.setAnchor(new Rectangle(100, 300, 200, 50));
		Hyperlink link2 = new Hyperlink();
		link2.setAddress(slideC);
		ppt.addHyperlink(link2);
		textBox2.setHyperlink(link2);
		slideA.addShape(textBox2);
		FileOutputStream out = new FileOutputStream("hyperlink.ppt");
		ppt.write(out);
		out.close();
	}
}

