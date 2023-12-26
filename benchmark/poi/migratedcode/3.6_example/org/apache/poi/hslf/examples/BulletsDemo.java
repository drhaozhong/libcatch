package org.apache.poi.hslf.examples;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import org.apache.poi.hslf.model.Shape;
import org.apache.poi.hslf.model.Sheet;
import org.apache.poi.hslf.model.Slide;
import org.apache.poi.hslf.model.TextBox;
import org.apache.poi.hslf.model.TextRun;
import org.apache.poi.hslf.model.TextShape;
import org.apache.poi.hslf.usermodel.RichTextRun;
import org.apache.poi.hslf.usermodel.SlideShow;


public final class BulletsDemo {
	public static void main(String[] args) throws Exception {
		SlideShow ppt = new SlideShow();
		Slide slide = ppt.createSlide();
		TextBox shape = new TextBox();
		RichTextRun rt = shape.getTextRun().getRichTextRuns()[0];
		shape.setText(("January\r" + (("February\r" + "March\r") + "April")));
		rt.setFontSize(42);
		rt.setBullet(true);
		rt.setBulletOffset(0);
		rt.setTextOffset(50);
		rt.setBulletChar('\u263a');
		slide.addShape(shape);
		shape.setAnchor(new Rectangle(50, 50, 500, 300));
		slide.addShape(shape);
		FileOutputStream out = new FileOutputStream("bullets.ppt");
		ppt.write(out);
		out.close();
	}
}

