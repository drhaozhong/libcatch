package org.apache.poi.hslf.examples;


import java.io.FileOutputStream;
import org.apache.poi.hslf.model.HeadersFooters;
import org.apache.poi.hslf.model.Slide;
import org.apache.poi.hslf.usermodel.SlideShow;


public class HeadersFootersDemo {
	public static void main(String[] args) throws Exception {
		SlideShow ppt = new SlideShow();
		HeadersFooters slideHeaders = ppt.getSlideHeadersFooters();
		slideHeaders.setFootersText("Created by POI-HSLF");
		slideHeaders.setSlideNumberVisible(true);
		slideHeaders.setDateTimeText("custom date time");
		HeadersFooters notesHeaders = ppt.getNotesHeadersFooters();
		notesHeaders.setFootersText("My notes footers");
		notesHeaders.setHeaderText("My notes header");
		Slide slide = ppt.createSlide();
		FileOutputStream out = new FileOutputStream("headers_footers.ppt");
		ppt.write(out);
		out.close();
	}
}

