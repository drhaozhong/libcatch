package org.apache.poi.xslf.usermodel.tutorial;


import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.sl.usermodel.PaintStyle;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSheet;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextParagraph;
import org.apache.poi.xslf.usermodel.XSLFTextRun;
import org.apache.poi.xslf.usermodel.XSLFTextShape;


public class Step1 {
	public static void main(String[] args) throws Exception {
		if ((args.length) == 0) {
			System.out.println("Input file is required");
			return;
		}
		FileInputStream fis = new FileInputStream(args[0]);
		XMLSlideShow ppt = new XMLSlideShow(fis);
		fis.close();
		for (XSLFSlide slide : ppt.getSlides()) {
			System.out.println(("Title: " + (slide.getTitle())));
			for (XSLFShape shape : slide.getShapes()) {
				if (shape instanceof XSLFTextShape) {
					XSLFTextShape tsh = ((XSLFTextShape) (shape));
					for (XSLFTextParagraph p : tsh) {
						System.out.println(("Paragraph level: " + (p.getIndentLevel())));
						for (XSLFTextRun r : p) {
							System.out.println(r.getRawText());
							System.out.println(("  bold: " + (r.isBold())));
							System.out.println(("  italic: " + (r.isItalic())));
							System.out.println(("  underline: " + (r.isUnderlined())));
							System.out.println(("  font.family: " + (r.getFontFamily())));
							System.out.println(("  font.size: " + (r.getFontSize())));
							System.out.println(("  font.color: " + (r.getFontColor())));
						}
					}
				}
			}
		}
		ppt.close();
	}
}

