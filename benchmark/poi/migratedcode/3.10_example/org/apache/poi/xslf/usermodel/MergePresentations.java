package org.apache.poi.xslf.usermodel;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import org.apache.poi.POIXMLDocument;


public final class MergePresentations {
	public static void main(String[] args) throws Exception {
		XMLSlideShow ppt = new XMLSlideShow();
		for (String arg : args) {
			FileInputStream is = new FileInputStream(arg);
			XMLSlideShow src = new XMLSlideShow(is);
			is.close();
			for (XSLFSlide srcSlide : src.getSlides()) {
				ppt.createSlide().importContent(srcSlide);
			}
		}
		FileOutputStream out = new FileOutputStream("merged.pptx");
		ppt.write(out);
		out.close();
	}
}

