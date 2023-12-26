package org.apache.poi.hslf.examples;


import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.geom.Rectangle2D;
import java.io.FileOutputStream;
import org.apache.poi.hslf.model.PPGraphics2D;
import org.apache.poi.hslf.usermodel.HSLFGroupShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;


public final class Graphics2DDemo {
	public static void main(String[] args) throws Exception {
		try (HSLFSlideShow ppt = new HSLFSlideShow()) {
			Object[] def = new Object[]{ Color.yellow, 40, Color.green, 60, Color.gray, 30, Color.red, 80 };
			HSLFSlide slide = ppt.createSlide();
			HSLFGroupShape group = new HSLFGroupShape();
			Rectangle bounds = new Rectangle(200, 100, 350, 300);
			group.setAnchor(bounds);
			group.setInteriorAnchor(new Rectangle(0, 0, 100, 100));
			slide.addShape(group);
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
			graphics.draw(group.getInteriorAnchor());
			graphics.drawString("Performance", (x + 30), (y + 10));
			try (FileOutputStream out = new FileOutputStream("hslf-graphics.ppt")) {
				ppt.write(out);
			}
		}
	}
}

