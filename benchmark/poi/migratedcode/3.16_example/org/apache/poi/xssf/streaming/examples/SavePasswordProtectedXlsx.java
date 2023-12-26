package org.apache.poi.xssf.streaming.examples;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.security.GeneralSecurityException;
import org.apache.poi.examples.util.TempFileUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.crypt.temp.EncryptedTempData;
import org.apache.poi.poifs.crypt.temp.SXSSFWorkbookWithCustomZipEntrySource;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;


public class SavePasswordProtectedXlsx {
	public static void main(String[] args) throws Exception {
		if ((args.length) != 2) {
			throw new IllegalArgumentException("Expected 2 params: filename and password");
		}
		TempFileUtils.checkTempFiles();
		String filename = args[0];
		String password = args[1];
		SXSSFWorkbookWithCustomZipEntrySource wb = new SXSSFWorkbookWithCustomZipEntrySource();
		try {
			for (int i = 0; i < 10; i++) {
				SXSSFSheet sheet = wb.createSheet(("Sheet" + i));
				for (int r = 0; r < 1000; r++) {
					SXSSFRow row = sheet.createRow(r);
					for (int c = 0; c < 100; c++) {
						SXSSFCell cell = row.createCell(c);
						cell.setCellValue("abcd");
					}
				}
			}
			EncryptedTempData tempData = new EncryptedTempData();
			try {
				wb.write(tempData.getOutputStream());
				SavePasswordProtectedXlsx.save(tempData.getInputStream(), filename, password);
				System.out.println(("Saved " + filename));
			} finally {
				tempData.dispose();
			}
		} finally {
			wb.close();
			wb.dispose();
		}
		TempFileUtils.checkTempFiles();
	}

	public static void save(final InputStream inputStream, final String filename, final String pwd) throws IOException, GeneralSecurityException, InvalidFormatException {
		POIFSFileSystem fs = null;
		FileOutputStream fos = null;
		OPCPackage opc = null;
		try {
			fs = new POIFSFileSystem();
			EncryptionInfo info = new EncryptionInfo(EncryptionMode.agile);
			Encryptor enc = Encryptor.getInstance(info);
			enc.confirmPassword(pwd);
			opc = OPCPackage.open(inputStream);
			fos = new FileOutputStream(filename);
			opc.save(enc.getDataStream(fs));
			fs.writeFilesystem(fos);
		} finally {
			IOUtils.closeQuietly(fos);
			IOUtils.closeQuietly(opc);
			IOUtils.closeQuietly(fs);
			IOUtils.closeQuietly(inputStream);
		}
	}
}

