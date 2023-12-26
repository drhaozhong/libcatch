package org.apache.poi.examples.util;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.poi.util.TempFile;


public class TempFileUtils {
	private TempFileUtils() {
	}

	public static void checkTempFiles() throws IOException {
		String tmpDir = (System.getProperty(TempFile.JAVA_IO_TMPDIR)) + "/poifiles";
		File tempDir = new File(tmpDir);
		if (tempDir.exists()) {
			String[] tempFiles = tempDir.list();
			if ((tempFiles != null) && ((tempFiles.length) > 0)) {
				System.out.println(("found files in poi temp dir " + (tempDir.getAbsolutePath())));
				for (String filename : tempFiles) {
					System.out.println(("file: " + filename));
				}
			}
		}else {
			System.out.println("unable to find poi temp dir");
		}
	}
}

