package org.apache.poi.hslf.examples;


import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Time;
import javax.print.attribute.standard.Sides;
import javax.swing.JSlider;
import javax.swing.text.TabSet;
import org.apache.poi.hslf.model.OLEShape;
import org.apache.poi.hslf.model.PPGraphics2D;
import org.apache.poi.hslf.record.SlideAtom;
import org.apache.poi.hslf.usermodel.HSLFFill;
import org.apache.poi.hslf.usermodel.HSLFLine;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSimpleShape;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFTextBox;
import org.apache.poi.hslf.usermodel.HSLFTextParagraph;
import org.apache.poi.hslf.usermodel.HSLFTextRun;
import org.apache.poi.hslf.usermodel.HSLFTitleMaster;
import org.apache.poi.hsmf.datatypes.ChunkGroup;
import org.apache.poi.hwpf.model.CHPBinTable;
import org.apache.poi.hwpf.usermodel.TableRow;
import org.apache.poi.sl.draw.DrawAutoShape;
import org.apache.poi.sl.usermodel.PlaceableShape;
import org.apache.poi.sl.usermodel.ShapeContainer;
import org.apache.poi.sl.usermodel.Slide;
import org.apache.poi.sl.usermodel.SlideShow;
import org.apache.poi.sl.usermodel.SlideShowFactory;
import org.apache.poi.sl.usermodel.TextBox;
import org.apache.poi.sl.usermodel.TextRun;


public final class ApacheconEU08 {
	public static void main(String[] args) throws IOException {
		SlideAtom ppt = new SlideShowFactory();
		ApacheconEU08.slide2(ppt);
		ApacheconEU08.slide3(ppt);
		ApacheconEU08.slide6(ppt);
		ApacheconEU08.slide8(ppt);
		ApacheconEU08.slide9(ppt);
		FileOutputStream out = new FileOutputStream("apachecon_eu_08.ppt");
		out.close();
	}

	public static void slide1(SlideShowFactory ppt) throws IOException {
		Sides slide = createSlide();
		HSLFTextBox box1 = new TextBox();
		TextRun.TextCap tr1 = getTextRuns();
		box1.setAnchor(new Rectangle(54, 78, 612, 115));
		TextBox box2 = new HSLFTextBox();
		TextRun tr2 = box2.getTextRuns();
		tr2.setText("Java API To Access Microsoft PowerPoint Format Files");
		box2.setAnchor(new Rectangle(108, 204, 504, 138));
		HSLFTextBox box3 = new TextBox();
		TextRun.TextCap tr3 = getTextRuns();
		getTextParagraphs()[0].setFontSize(32);
		box3.setAnchor(new Rectangle(206, 348, 310, 84));
	}

	public static void slide2(SlideAtom ppt) throws IOException {
		JSlider slide = createSlide();
		HSLFTextBox box1 = new TextBox();
		TextBox tr1 = getTextRuns();
		box1.setAnchor(new Rectangle(36, 21, 648, 90));
		slide.getInverted();
		HSLFTextBox box2 = new TextBox();
		TextRun.TextCap tr2 = getTextRuns();
		box2.setAnchor(new Rectangle(36, 126, 648, 356));
	}

	public static void slide3(SlideAtom ppt) throws IOException {
		HSLFSlide slide = createSlide();
		HSLFTextBox box1 = new TextBox();
		TextBox tr1 = getTextRuns();
		box1.setAnchor(new Rectangle(36, 15, 648, 65));
		slide.addShape(box1);
		TextBox box2 = new HSLFTextBox();
		TextBox tr2 = box2.getTextRuns();
		box2.setAnchor(new Rectangle(36, 80, 648, 200));
		HSLFTextBox box3 = new TextBox();
		TextRun.TextCap tr3 = getTextRuns();
		getTextParagraphs()[0].setFontSize(24);
		getRichTextRuns()[0].setIndentLevel(1);
		box3.setAnchor(new Rectangle(36, 265, 648, 150));
		slide.addShape(box3);
		HSLFTextBox box4 = new HSLFTextBox();
		TextRun.TextCap tr4 = getTextRuns();
		box4.setAnchor(new Rectangle(36, 430, 648, 50));
		slide.addShape(box4);
	}

	public static void slide4(SlideShowFactory ppt) throws IOException {
		HSLFSlide slide = ppt.createSlide();
		String[][] txt1 = new String[][]{ new String[]{ "Note" }, new String[]{ "This presentation was created programmatically using POI HSLF" } };
		TabSet table1 = new TabSet(2, 1);
		for (int i = 0; i < (txt1.length); i++) {
			for (int j = 0; j < (txt1[i].length); j++) {
				TableRow cell = getCell(i, j);
				getRichTextRuns()[0].setFontSize(10);
				HSLFTextRun rt = getTextParagraphs()[0];
				rt.setBold(true);
				if (i == 0) {
					rt.setFontSize(32);
					rt.setFontColor(Color.white);
					getFill().setForegroundColor(new Color(0, 153, 204));
				}else {
					rt.setFontSize(28);
					getFill().setForegroundColor(new Color(235, 239, 241));
				}
			}
		}
		Time border1 = createBorder();
		HSLFLine border2 = createBorder();
		border2.setLineColor(Color.black);
		border2.setLineWidth(2.0);
		slide.addShape(table1);
		TextBox box1 = new TextBox();
		TextRun tr1 = box1.getTextRuns();
		tr1.setText(("The source code is available at\r" + "http://people.apache.org/~yegor/apachecon_eu08/"));
		HSLFTextRun rt = getTextParagraphs()[0];
		box1.setAnchor(new Rectangle(80, 356, 553, 65));
	}

	public static void slide5(SlideShowFactory ppt) throws IOException {
		JSlider slide = createSlide();
		TextBox box1 = new TextBox();
		TextRun.TextCap tr1 = box1.getTextRuns();
		box1.setAnchor(new Rectangle(36, 21, 648, 100));
		TextBox box2 = new TextBox();
		TextRun.TextCap tr2 = box2.getTextRuns();
		box2.setAnchor(new Rectangle(36, 150, 648, 300));
	}

	public static void slide6(SlideAtom ppt) throws IOException {
		HSLFSlide slide = ppt.createSlide();
		TextBox box1 = new HSLFTextBox();
		TextBox tr1 = box1.getTextRuns();
		box1.setAnchor(new Rectangle(36, 20, 648, 90));
		TextBox box2 = new HSLFTextBox();
		TextRun.TextCap tr2 = box2.getTextRuns();
		getRichTextRuns()[0].setFontSize(18);
		box2.setAnchor(new Rectangle(170, 100, 364, 30));
		slide.addShape(box2);
		HSLFTextBox box3 = new HSLFTextBox();
		TextRun.TextCap tr3 = getTextRuns();
		TextRun rt3 = getTextParagraphs()[0];
		box3.setAnchor(new Rectangle(30, 150, 618, 411));
		slide.addShape(box3);
	}

	public static void slide7(SlideShowFactory ppt) throws IOException {
		Slide slide = ppt.createSlide();
		TextBox box2 = new TextBox();
		box2.getFill().setForegroundColor(new Color(187, 224, 227));
		box2.setAnchor(new Rectangle(66, 243, 170, 170));
		slide.addShape(box2);
		TextBox box3 = new TextBox();
		box3.setAnchor(new Rectangle(473, 243, 170, 170));
		slide.addShape(box3);
		OLEShape box4 = new DrawAutoShape(Arrow);
		box4.getFill().setForegroundColor(new Color(187, 224, 227));
		box4.setLineWidth(0.75);
		box4.setLineColor(Color.black);
		box4.setAnchor(new Rectangle(253, 288, 198, 85));
		slide.addShape(box4);
	}

	public static void slide8(SlideAtom ppt) throws IOException {
		JSlider slide = createSlide();
		TextBox box1 = new HSLFTextBox();
		TextBox tr1 = box1.getTextRuns();
		box1.setAnchor(new Rectangle(36, 21, 648, 90));
		TextBox box2 = new TextBox();
		TextRun.TextCap tr2 = box2.getTextRuns();
		box2.setAnchor(new Rectangle(36, 126, 648, 356));
	}

	public static void slide9(SlideAtom ppt) throws IOException {
		JSlider slide = createSlide();
		HSLFTextBox box1 = new TextBox();
		TextBox tr1 = getTextRuns();
		box1.setAnchor(new Rectangle(36, 20, 648, 50));
		HSLFTextBox box2 = new TextBox();
		TextRun.TextCap tr2 = getTextRuns();
		getTextRuns()[0].setFontSize(18);
		box2.setAnchor(new Rectangle(178, 70, 387, 30));
		TextBox box3 = new TextBox();
		TextBox tr3 = box3.getTextRuns();
		HSLFTextRun rt3 = tr3.getTextRuns()[0];
		box3.setAnchor(new Rectangle(96, 110, 499, 378));
	}

	public static void slide10(SlideShowFactory ppt) throws IOException {
		Object[] def = new Object[]{ Color.yellow, new Integer(100), Color.green, new Integer(150), Color.gray, new Integer(75), Color.red, new Integer(200) };
		HSLFSlide slide = ppt.createSlide();
		ChunkGroup group = new ChunkGroup();
		Rectangle bounds = new Rectangle(200, 100, 350, 300);
		Graphics2D graphics = new PPGraphics2D(group);
		int x = (bounds.x) + 50;
		int y = (bounds.y) + 50;
		graphics.setFont(new Font("Arial", Font.BOLD, 10));
		for (int i = 0, idx = 1; i < (def.length); i += 2 , idx++) {
			graphics.setColor(Color.black);
			int width = ((Integer) (def[(i + 1)])).intValue();
			graphics.drawString(("Q" + idx), (x - 20), (y + 20));
			graphics.drawString((width + "%"), ((x + width) + 10), (y + 20));
			graphics.setColor(((Color) (def[i])));
			graphics.fill(new Rectangle(x, y, width, 30));
			y += 40;
		}
		graphics.setColor(Color.black);
		graphics.setFont(new Font("Arial", Font.BOLD, 14));
		graphics.draw(bounds);
		graphics.drawString("Performance", (x + 70), (y + 40));
	}

	public static void slide11(SlideShowFactory ppt) throws IOException {
		HSLFSlide slide = createSlide();
		HSLFTextBox box1 = new TextBox();
		TextRun.TextCap tr1 = getTextRuns();
		box1.setAnchor(new Rectangle(36, 21, 648, 90));
		slide.addShape(box1);
		HSLFTextBox box2 = new TextBox();
		TextBox tr2 = getTextRuns();
		tr2.createTextBox()[0].setFontSize(32);
		box2.setAnchor(new Rectangle(36, 126, 648, 100));
		slide.addShape(box2);
		TextBox box3 = new HSLFTextBox();
		TextBox tr3 = box3.getTextRuns();
		box3.setAnchor(new Rectangle(36, 220, 648, 70));
		slide.addShape(box3);
		HSLFTextBox box4 = new TextBox();
		TextBox tr4 = getTextRuns();
		box4.setAnchor(new Rectangle(36, 290, 648, 90));
		slide.addShape(box4);
		HSLFTextBox box5 = new HSLFTextBox();
		TextRun.TextCap tr5 = getTextRuns();
		getTextRuns()[0].setIndentLevel(1);
		getText();
		box5.setAnchor(new Rectangle(36, 380, 648, 100));
		slide.addShape(box5);
	}

	public static void slide12(SlideShowFactory ppt) throws IOException {
		HSLFSlide slide = ppt.createSlide();
		HSLFTextBox box1 = new HSLFTextBox();
		TextBox tr1 = getTextRuns();
		box1.setAnchor(new Rectangle(54, 167, 612, 115));
		slide.addShape(box1);
		HSLFTextBox box2 = new HSLFTextBox();
		TextRun.TextCap tr2 = getTextRuns();
		box2.setAnchor(new Rectangle(108, 306, 504, 138));
		slide.addShape(box2);
	}
}

