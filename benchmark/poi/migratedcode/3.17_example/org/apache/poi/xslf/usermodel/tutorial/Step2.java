package org.apache.poi.xslf.usermodel.tutorial;


import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.xslf.usermodel.SlideLayout;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFSheet;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFSlideLayout;
import org.apache.poi.xslf.usermodel.XSLFSlideMaster;
import org.apache.poi.xslf.usermodel.XSLFTextParagraph;
import org.apache.poi.xslf.usermodel.XSLFTextRun;
import org.apache.poi.xslf.usermodel.XSLFTextShape;


public class Step2 {
	public static void main(String[] args) throws Exception {
		XMLSlideShow ppt = new XMLSlideShow();
		System.out.println("Available slide layouts:");
		for (XSLFSlideMaster master : ppt.getSlideMasters()) {
			for (XSLFSlideLayout layout : master.getSlideLayouts()) {
				System.out.println(layout.getType());
			}
		}
		ppt.createSlide();
		XSLFSlideMaster defaultMaster = ppt.getSlideMasters().get(0);
		XSLFSlideLayout titleLayout = defaultMaster.getLayout(SlideLayout.TITLE);
		XSLFSlide slide1 = ppt.createSlide(titleLayout);
		XSLFTextShape title1 = slide1.getPlaceholder(0);
		title1.setText("First Title");
		XSLFSlideLayout titleBodyLayout = defaultMaster.getLayout(SlideLayout.TITLE_AND_CONTENT);
		XSLFSlide slide2 = ppt.createSlide(titleBodyLayout);
		XSLFTextShape title2 = slide2.getPlaceholder(0);
		title2.setText("Second Title");
		XSLFTextShape body2 = slide2.getPlaceholder(1);
		body2.clearText();
		body2.addNewTextParagraph().addNewTextRun().setText("First paragraph");
		body2.addNewTextParagraph().addNewTextRun().setText("Second paragraph");
		body2.addNewTextParagraph().addNewTextRun().setText("Third paragraph");
		FileOutputStream out = new FileOutputStream("step2.pptx");
		ppt.write(out);
		out.close();
		ppt.close();
	}
}

