package org.apache.poi.hslf.examples;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import org.apache.poi.hslf.record.SlideAtom;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.sl.usermodel.PlaceableShape;
import org.apache.poi.sl.usermodel.SlideShowFactory;
import org.apache.poi.sl.usermodel.TextBox;
import org.apache.poi.sl.usermodel.TextRun;


public final class BulletsDemo {
	public static void main(String[] args) throws Exception {
		SlideAtom ppt = new SlideShowFactory();
		HSLFSlide slide = createSlide();
		TextBox shape = new TextBox();
		TextRun rt = shape.getTextRuns().getRichTextRuns()[0];
		shape.setAnchor(new Rectangle(50, 50, 500, 300));
		FileOutputStream out = new FileOutputStream("bullets.ppt");
		out.close();
	}
}

