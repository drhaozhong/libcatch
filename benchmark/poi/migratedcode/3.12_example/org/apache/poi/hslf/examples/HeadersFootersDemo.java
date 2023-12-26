package org.apache.poi.hslf.examples;


import java.io.FileOutputStream;
import org.apache.poi.hslf.model.HeadersFooters;
import org.apache.poi.hslf.record.SlideAtom;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.sl.usermodel.SlideShowFactory;
import org.apache.poi.xssf.usermodel.extensions.XSSFHeaderFooter;


public class HeadersFootersDemo {
	public static void main(String[] args) throws Exception {
		SlideAtom ppt = new SlideShowFactory();
		HeadersFooters slideHeaders = getHeaderFooter();
		slideHeaders.setFootersText("Created by POI-HSLF");
		slideHeaders.setSlideNumberVisible(true);
		slideHeaders.setDateTimeText("custom date time");
		HeadersFooters notesHeaders = getHeaderFooter();
		notesHeaders.setFootersText("My notes footers");
		notesHeaders.setHeaderText("My notes header");
		HSLFSlide slide = createSlide();
		FileOutputStream out = new FileOutputStream("headers_footers.ppt");
		out.close();
	}
}

