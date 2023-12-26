package org.apache.poi.xssf.usermodel.examples;


import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import org.apache.poi.crypt.examples.EncryptionUtils;
import org.apache.poi.examples.util.TempFileUtils;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.temp.AesZipFileZipEntrySource;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class LoadPasswordProtectedXlsx {
	public static void main(String[] args) throws Exception {
		if ((args.length) != 2) {
			throw new IllegalArgumentException("Expected 2 params: filename and password");
		}
		TempFileUtils.checkTempFiles();
		String filename = args[0];
		String password = args[1];
		try (FileInputStream fis = new FileInputStream(filename);InputStream unencryptedStream = EncryptionUtils.decrypt(fis, password)) {
			LoadPasswordProtectedXlsx.printSheetCount(unencryptedStream);
		}
		TempFileUtils.checkTempFiles();
	}

	public static void printSheetCount(final InputStream inputStream) throws Exception {
		try (AesZipFileZipEntrySource source = AesZipFileZipEntrySource.createZipEntrySource(inputStream);OPCPackage pkg = OPCPackage.open(source);XSSFWorkbook workbook = new XSSFWorkbook(pkg)) {
			System.out.println(("sheet count: " + (workbook.getNumberOfSheets())));
		}
	}
}

