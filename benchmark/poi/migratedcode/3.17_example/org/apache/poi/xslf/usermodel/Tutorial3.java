package org.apache.poi.xslf.usermodel;


import java.awt.Rectangle;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.sl.usermodel.Placeholder;


public class Tutorial3 {
	public static void main(String[] args) throws IOException {
		XMLSlideShow ppt = new XMLSlideShow();
		try {
			XSLFSlide slide = ppt.createSlide();
			XSLFTextShape titleShape = slide.createTextBox();
			titleShape.setPlaceholder(Placeholder.TITLE);
			titleShape.setText("This is a slide title");
			titleShape.setAnchor(new Rectangle(50, 50, 400, 100));
			FileOutputStream out = new FileOutputStream("title.pptx");
			ppt.write(out);
			out.close();
		} finally {
			ppt.close();
		}
	}
}

