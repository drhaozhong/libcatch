package org.apache.poi.xssf.usermodel.examples;


import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import org.apache.poi.crypt.examples.EncryptionUtils;
import org.apache.poi.examples.util.TempFileUtils;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.temp.AesZipFileZipEntrySource;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class LoadPasswordProtectedXlsx {
	public static void main(String[] args) throws Exception {
		if ((args.length) != 2) {
			throw new IllegalArgumentException("Expected 2 params: filename and password");
		}
		TempFileUtils.checkTempFiles();
		String filename = args[0];
		String password = args[1];
		FileInputStream fis = new FileInputStream(filename);
		try {
			InputStream unencryptedStream = EncryptionUtils.decrypt(fis, password);
			try {
				LoadPasswordProtectedXlsx.printSheetCount(unencryptedStream);
			} finally {
				IOUtils.closeQuietly(unencryptedStream);
			}
		} finally {
			IOUtils.closeQuietly(fis);
		}
		TempFileUtils.checkTempFiles();
	}

	public static void printSheetCount(final InputStream inputStream) throws Exception {
		AesZipFileZipEntrySource source = AesZipFileZipEntrySource.createZipEntrySource(inputStream);
		try {
			OPCPackage pkg = OPCPackage.open(source);
			try {
				XSSFWorkbook workbook = new XSSFWorkbook(pkg);
				try {
					System.out.println(("sheet count: " + (workbook.getNumberOfSheets())));
				} finally {
					IOUtils.closeQuietly(workbook);
				}
			} finally {
				IOUtils.closeQuietly(pkg);
			}
		} finally {
			IOUtils.closeQuietly(source);
		}
	}
}

