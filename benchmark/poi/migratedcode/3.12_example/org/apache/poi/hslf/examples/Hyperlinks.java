package org.apache.poi.hslf.examples;


import java.io.FileInputStream;
import java.io.PrintStream;
import javax.swing.JSlider;
import org.apache.poi.hslf.model.Hyperlink;
import org.apache.poi.hslf.record.SlideAtom;
import org.apache.poi.hslf.usermodel.HSLFGroupShape;
import org.apache.poi.hslf.usermodel.HSLFHyperlink;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hssf.extractor.ExcelExtractor;
import org.apache.poi.sl.usermodel.TextBox;


public final class Hyperlinks {
	public static void main(String[] args) throws Exception {
		for (int i = 0; i < (args.length); i++) {
			FileInputStream is = new FileInputStream(args[i]);
			SlideAtom ppt = new SlideAtom();
			is.close();
			JSlider[] slide = getSlides();
			for (int j = 0; j < (slide.length); j++) {
				System.out.println(("slide " + (slide[j].getMinimum())));
				System.out.println("reading hyperlinks from the text runs");
				TextBox[] txt = getTextRuns();
				for (int k = 0; k < (txt.length); k++) {
					String text = txt[k].getText();
					HSLFHyperlink[] links = txt[k].getHyperlinks();
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
				HSLFShape[] sh = getShapes();
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

