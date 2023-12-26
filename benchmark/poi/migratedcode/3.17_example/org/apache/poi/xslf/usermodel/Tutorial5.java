package org.apache.poi.xslf.usermodel;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.sl.usermodel.PictureData;

import static org.apache.poi.sl.usermodel.PictureData.PictureType.PNG;


public class Tutorial5 {
	public static void main(String[] args) throws IOException {
		XMLSlideShow ppt = new XMLSlideShow();
		try {
			XSLFSlide slide = ppt.createSlide();
			File img = new File(System.getProperty("POI.testdata.path", "test-data"), "slideshow/clock.jpg");
			XSLFPictureData pictureData = ppt.addPicture(img, PNG);
			slide.createPicture(pictureData);
			FileOutputStream out = new FileOutputStream("images.pptx");
			ppt.write(out);
			out.close();
		} finally {
			ppt.close();
		}
	}
}

