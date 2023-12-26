package org.apache.poi.hslf.examples;


import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Locale;
import org.apache.poi.hslf.usermodel.HSLFHyperlink;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSimpleShape;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTextParagraph;
import org.apache.poi.hslf.usermodel.HSLFTextRun;


public final class Hyperlinks {
	public static void main(String[] args) throws Exception {
		for (int i = 0; i < (args.length); i++) {
			FileInputStream is = new FileInputStream(args[i]);
			HSLFSlideShow ppt = new HSLFSlideShow(is);
			is.close();
			for (HSLFSlide slide : ppt.getSlides()) {
				System.out.println(("\nslide " + (slide.getSlideNumber())));
				System.out.println("- reading hyperlinks from the text runs");
				for (List<HSLFTextParagraph> paras : slide.getTextParagraphs()) {
					for (HSLFTextParagraph para : paras) {
						for (HSLFTextRun run : para) {
							HSLFHyperlink link = run.getHyperlink();
							if (link != null) {
								System.out.println(Hyperlinks.toStr(link, run.getRawText()));
							}
						}
					}
				}
				System.out.println("- reading hyperlinks from the slide's shapes");
				for (HSLFShape sh : slide.getShapes()) {
					if (sh instanceof HSLFSimpleShape) {
						HSLFHyperlink link = ((HSLFSimpleShape) (sh)).getHyperlink();
						if (link != null) {
							System.out.println(Hyperlinks.toStr(link, null));
						}
					}
				}
			}
			ppt.close();
		}
	}

	static String toStr(HSLFHyperlink link, String rawText) {
		String formatStr = "title: %1$s, address: %2$s" + (rawText == null ? "" : ", start: %3$s, end: %4$s, substring: %5$s");
		return String.format(Locale.ROOT, formatStr, link.getLabel(), link.getAddress(), link.getStartIndex(), link.getEndIndex(), rawText);
	}
}

