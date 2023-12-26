package org.apache.poi.hslf.examples;


import java.io.FileOutputStream;
import org.apache.poi.hslf.model.HeadersFooters;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;


public class HeadersFootersDemo {
	public static void main(String[] args) throws Exception {
		HSLFSlideShow ppt = new HSLFSlideShow();
		HeadersFooters slideHeaders = ppt.getSlideHeadersFooters();
		slideHeaders.setFootersText("Created by POI-HSLF");
		slideHeaders.setSlideNumberVisible(true);
		slideHeaders.setDateTimeText("custom date time");
		HeadersFooters notesHeaders = ppt.getNotesHeadersFooters();
		notesHeaders.setFootersText("My notes footers");
		notesHeaders.setHeaderText("My notes header");
		HSLFSlide slide = ppt.createSlide();
		FileOutputStream out = new FileOutputStream("headers_footers.ppt");
		ppt.write(out);
		out.close();
	}
}

