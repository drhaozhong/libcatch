package org.apache.poi.poifs.poibrowser;


import java.awt.Color;
import java.awt.Component;
import javax.swing.JComponent;


public class Util {
	public static void invert(JComponent c) {
		Color invBackground = c.getForeground();
		Color invForeground = c.getBackground();
		c.setBackground(invBackground);
		c.setForeground(invForeground);
	}
}

