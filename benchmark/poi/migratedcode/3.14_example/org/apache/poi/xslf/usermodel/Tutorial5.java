package org.apache.poi.xslf.usermodel;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.sl.usermodel.PictureData;
import org.apache.poi.util.IOUtils;

import static org.apache.poi.sl.usermodel.PictureData.PictureType.PNG;


public class Tutorial5 {
	public static void main(String[] args) throws IOException {
		XMLSlideShow ppt = new XMLSlideShow();
		XSLFSlide slide = ppt.createSlide();
		File img = new File(System.getProperty("POI.testdata.path"), "slideshow/clock.jpg");
		byte[] data = IOUtils.toByteArray(new FileInputStream(img));
		XSLFPictureData pictureIndex = ppt.addPicture(data, PNG);
		slide.createPicture(pictureIndex);
		FileOutputStream out = new FileOutputStream("images.pptx");
		ppt.write(out);
		out.close();
		ppt.close();
	}
}

