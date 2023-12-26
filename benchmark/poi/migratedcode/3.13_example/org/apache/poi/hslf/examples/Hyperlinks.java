package org.apache.poi.hslf.examples;


import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.hslf.usermodel.HSLFHyperlink;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTextParagraph;


public final class Hyperlinks {
	public static void main(String[] args) throws Exception {
		for (int i = 0; i < (args.length); i++) {
			FileInputStream is = new FileInputStream(args[i]);
			HSLFSlideShow ppt = new HSLFSlideShow(is);
			is.close();
			for (HSLFSlide slide : ppt.getSlides()) {
				System.out.println(("\nslide " + (slide.getSlideNumber())));
				System.out.println("- reading hyperlinks from the text runs");
				for (List<HSLFTextParagraph> txtParas : slide.getTextParagraphs()) {
					List<HSLFHyperlink> links = HSLFHyperlink.find(txtParas);
					String text = HSLFTextParagraph.getRawText(txtParas);
					for (HSLFHyperlink link : links) {
						System.out.println(Hyperlinks.toStr(link, text));
					}
				}
				System.out.println("- reading hyperlinks from the slide's shapes");
				for (HSLFShape sh : slide.getShapes()) {
					HSLFHyperlink link = HSLFHyperlink.find(sh);
					if (link == null)
						continue;

					System.out.println(Hyperlinks.toStr(link, null));
				}
			}
		}
	}

	static String toStr(HSLFHyperlink link, String rawText) {
		String formatStr = "title: %1$s, address: %2$s" + (rawText == null ? "" : ", start: %3$s, end: %4$s, substring: %5$s");
		String substring = (rawText == null) ? "" : rawText.substring(link.getStartIndex(), ((link.getEndIndex()) - 1));
		return String.format(formatStr, link.getLabel(), link.getAddress(), link.getStartIndex(), link.getEndIndex(), substring);
	}
}

