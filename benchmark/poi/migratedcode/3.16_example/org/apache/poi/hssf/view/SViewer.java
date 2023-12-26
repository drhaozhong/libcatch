package org.apache.poi.hssf.view;


import java.applet.Applet;
import java.awt.AWTEvent;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.event.WindowEvent;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import javax.swing.JApplet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;


public class SViewer extends JApplet {
	private SViewerPanel panel;

	boolean isStandalone = false;

	String filename = null;

	public String getParameter(String key, String def) {
		return isStandalone ? System.getProperty(key, def) : (getParameter(key)) != null ? getParameter(key) : def;
	}

	public SViewer() {
	}

	@Override
	public void init() {
		try {
			jbInit();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void jbInit() throws Exception {
		InputStream i = null;
		boolean isurl = false;
		if ((filename) == null)
			filename = getParameter("filename");

		if (((filename) == null) || (filename.substring(0, 7).equals("http://"))) {
			isurl = true;
			if ((filename) == null)
				filename = getParameter("url");

			i = getXLSFromURL(filename);
		}
		HSSFWorkbook wb = null;
		if (isurl) {
			wb = constructWorkbook(i);
		}else {
			wb = constructWorkbook(filename);
		}
		panel = new SViewerPanel(wb, false);
		getContentPane().setLayout(new BorderLayout());
		getContentPane().add(panel, BorderLayout.CENTER);
	}

	private HSSFWorkbook constructWorkbook(String filename) throws FileNotFoundException, IOException {
		HSSFWorkbook wb = null;
		FileInputStream in = new FileInputStream(filename);
		wb = new HSSFWorkbook(in);
		in.close();
		return wb;
	}

	private HSSFWorkbook constructWorkbook(InputStream in) throws IOException {
		HSSFWorkbook wb = null;
		wb = new HSSFWorkbook(in);
		in.close();
		return wb;
	}

	@Override
	public void start() {
	}

	@Override
	public void stop() {
	}

	@Override
	public void destroy() {
	}

	@Override
	public String getAppletInfo() {
		return "Applet Information";
	}

	@Override
	public String[][] getParameterInfo() {
		return null;
	}

	private InputStream getXLSFromURL(String urlstring) throws IOException, MalformedURLException {
		URL url = new URL(urlstring);
		URLConnection uc = url.openConnection();
		String field = uc.getHeaderField(0);
		for (int i = 0; field != null; i++) {
			System.out.println(field);
			field = uc.getHeaderField(i);
		}
		BufferedInputStream is = new BufferedInputStream(uc.getInputStream());
		return is;
	}

	public static void main(String[] args) {
		if ((args.length) < 1) {
			throw new IllegalArgumentException("A filename to view must be supplied as the first argument, but none was given");
		}
		SViewer applet = new SViewer();
		applet.isStandalone = true;
		applet.filename = args[0];
		Frame frame;
		frame = new Frame() {
			@Override
			protected void processWindowEvent(WindowEvent e) {
				super.processWindowEvent(e);
				if ((e.getID()) == (WindowEvent.WINDOW_CLOSING)) {
					System.exit(0);
				}
			}

			@Override
			public synchronized void setTitle(String title) {
				super.setTitle(title);
				enableEvents(AWTEvent.WINDOW_EVENT_MASK);
			}
		};
		frame.setTitle("Applet Frame");
		frame.add(applet, BorderLayout.CENTER);
		applet.init();
		applet.start();
		frame.setSize(400, 320);
		Dimension d = Toolkit.getDefaultToolkit().getScreenSize();
		frame.setLocation((((d.width) - (frame.getSize().width)) / 2), (((d.height) - (frame.getSize().height)) / 2));
		frame.setVisible(true);
	}
}

