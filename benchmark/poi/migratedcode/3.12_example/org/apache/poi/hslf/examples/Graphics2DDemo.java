package org.apache.poi.hslf.examples;


import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.io.FileOutputStream;
import org.apache.poi.hslf.model.PPGraphics2D;
import org.apache.poi.hslf.record.SlideAtom;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hssf.model.WorkbookRecordList;
import org.apache.poi.sl.usermodel.Shadow;


public final class Graphics2DDemo {
	public static void main(String[] args) throws Exception {
		SlideAtom ppt = new SlideAtom();
		Object[] def = new Object[]{ Color.yellow, new Integer(40), Color.green, new Integer(60), Color.gray, new Integer(30), Color.red, new Integer(80) };
		HSLFSlide slide = createSlide();
		Shadow group = new Shadow();
		Rectangle bounds = new Rectangle(200, 100, 350, 300);
		Graphics2D graphics = new PPGraphics2D(group);
		int x = 10;
		int y = 10;
		graphics.setFont(new Font("Arial", Font.BOLD, 10));
		for (int i = 0, idx = 1; i < (def.length); i += 2 , idx++) {
			graphics.setColor(Color.black);
			int width = ((Integer) (def[(i + 1)])).intValue();
			graphics.drawString(("Q" + idx), (x - 5), (y + 10));
			graphics.drawString((width + "%"), ((x + width) + 3), (y + 10));
			graphics.setColor(((Color) (def[i])));
			graphics.fill(new Rectangle(x, y, width, 10));
			y += 15;
		}
		graphics.setColor(Color.black);
		graphics.setFont(new Font("Arial", Font.BOLD, 14));
		graphics.draw(group.getFontpos());
		graphics.drawString("Performance", (x + 30), (y + 10));
		FileOutputStream out = new FileOutputStream("hslf-graphics.ppt");
		out.close();
	}
}

