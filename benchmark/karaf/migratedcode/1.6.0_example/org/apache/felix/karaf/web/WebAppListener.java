package org.apache.felix.karaf.web;


import java.io.File;
import java.io.PrintStream;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.apache.karaf.client.Main;


public class WebAppListener implements ServletContextListener {
	private Main main;

	public void contextInitialized(ServletContextEvent sce) {
		try {
			System.err.println("contextInitialized");
			String root = new File(((sce.getServletContext().getRealPath("/")) + "WEB-INF/karaf")).getAbsolutePath();
			System.err.println(("Root: " + root));
			System.setProperty("karaf.home", root);
			System.setProperty("karaf.base", root);
			System.setProperty("karaf.startLocalConsole", "false");
			System.setProperty("karaf.startRemoteShell", "true");
			this.main = new org.apache.felix.karaf.main.Main(new String[0]);
		} catch (Exception e) {
			main = null;
			e.printStackTrace();
		}
	}

	public void contextDestroyed(ServletContextEvent sce) {
		try {
			System.err.println("contextDestroyed");
			if ((main) != null) {
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

