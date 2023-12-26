package org.apache.poi.xslf.usermodel;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.xslf.usermodel.XSLFSlide;


public final class MergePresentations {
	public static void main(String[] args) throws Exception {
		XMLSlideShow ppt = new XMLSlideShow();
		try {
			for (String arg : args) {
				FileInputStream is = new FileInputStream(arg);
				XMLSlideShow src = new XMLSlideShow(is);
				is.close();
				for (XSLFSlide srcSlide : src.getSlides()) {
					ppt.createSlide().importContent(srcSlide);
				}
				src.close();
			}
			FileOutputStream out = new FileOutputStream("merged.pptx");
			ppt.write(out);
			out.close();
		} finally {
			ppt.close();
		}
	}
}

