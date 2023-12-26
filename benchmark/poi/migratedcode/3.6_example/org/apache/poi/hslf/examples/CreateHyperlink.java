package org.apache.poi.hslf.examples;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import org.apache.poi.hslf.model.Hyperlink;
import org.apache.poi.hslf.model.Shape;
import org.apache.poi.hslf.model.Sheet;
import org.apache.poi.hslf.model.Slide;
import org.apache.poi.hslf.model.TextBox;
import org.apache.poi.hslf.model.TextShape;
import org.apache.poi.hslf.usermodel.SlideShow;


public final class CreateHyperlink {
	public static void main(String[] args) throws Exception {
		SlideShow ppt = new SlideShow();
		Slide slide = ppt.createSlide();
		TextBox shape = new TextBox();
		shape.setText("Apache POI");
		Rectangle anchor = new Rectangle(100, 100, 200, 50);
		shape.setAnchor(anchor);
		String text = shape.getText();
		Hyperlink link = new Hyperlink();
		link.setAddress("http://www.apache.org");
		link.setTitle(shape.getText());
		int linkId = ppt.addHyperlink(link);
		shape.setHyperlink(linkId, 0, text.length());
		slide.addShape(shape);
		FileOutputStream out = new FileOutputStream("hyperlink.ppt");
		ppt.write(out);
		out.close();
	}
}

