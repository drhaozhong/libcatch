package org.apache.poi.xslf.usermodel;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.xslf.usermodel.XSLFSlide;


public final class MergePresentations {
	public static void main(String[] args) throws Exception {
		try (XMLSlideShow ppt = new XMLSlideShow()) {
			for (String arg : args) {
				try (FileInputStream is = new FileInputStream(arg);XMLSlideShow src = new XMLSlideShow(is)) {
					for (XSLFSlide srcSlide : src.getSlides()) {
						ppt.createSlide().importContent(srcSlide);
					}
				}
			}
			try (FileOutputStream out = new FileOutputStream("merged.pptx")) {
				ppt.write(out);
			}
		}
	}
}

