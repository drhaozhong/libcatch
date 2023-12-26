package org.apache.poi.hslf.examples;


import java.io.FileInputStream;
import java.io.PrintStream;
import org.apache.poi.hslf.model.Hyperlink;
import org.apache.poi.hslf.model.Shape;
import org.apache.poi.hslf.model.Sheet;
import org.apache.poi.hslf.model.Slide;
import org.apache.poi.hslf.model.TextRun;
import org.apache.poi.hslf.usermodel.SlideShow;


public final class Hyperlinks {
	public static void main(String[] args) throws Exception {
		for (int i = 0; i < (args.length); i++) {
			FileInputStream is = new FileInputStream(args[i]);
			SlideShow ppt = new SlideShow(is);
			is.close();
			Slide[] slide = ppt.getSlides();
			for (int j = 0; j < (slide.length); j++) {
				System.out.println(("slide " + (slide[j].getSlideNumber())));
				System.out.println("reading hyperlinks from the text runs");
				TextRun[] txt = slide[j].getTextRuns();
				for (int k = 0; k < (txt.length); k++) {
					String text = txt[k].getText();
					Hyperlink[] links = txt[k].getHyperlinks();
					if (links != null)
						for (int l = 0; l < (links.length); l++) {
							Hyperlink link = links[l];
							String title = link.getTitle();
							String address = link.getAddress();
							System.out.println(("  " + title));
							System.out.println(("  " + address));
							String substring = text.substring(link.getStartIndex(), ((link.getEndIndex()) - 1));
							System.out.println(("  " + substring));
						}

				}
				System.out.println("  reading hyperlinks from the slide's shapes");
				Shape[] sh = slide[j].getShapes();
				for (int k = 0; k < (sh.length); k++) {
					Hyperlink link = sh[k].getHyperlink();
					if (link != null) {
						String title = link.getTitle();
						String address = link.getAddress();
						System.out.println(("  " + title));
						System.out.println(("  " + address));
					}
				}
			}
		}
	}
}

