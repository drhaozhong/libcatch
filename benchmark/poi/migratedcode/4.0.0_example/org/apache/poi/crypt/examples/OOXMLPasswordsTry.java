package org.apache.poi.crypt.examples;


import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.security.GeneralSecurityException;
import org.apache.poi.poifs.crypt.Decryptor;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;


public class OOXMLPasswordsTry implements Closeable {
	private POIFSFileSystem fs;

	private EncryptionInfo info;

	private Decryptor d;

	private OOXMLPasswordsTry(POIFSFileSystem fs) throws IOException {
		info = new EncryptionInfo(fs);
		d = Decryptor.getInstance(info);
		this.fs = fs;
	}

	private OOXMLPasswordsTry(File file) throws IOException {
		this(new POIFSFileSystem(file, true));
	}

	private OOXMLPasswordsTry(InputStream is) throws IOException {
		this(new POIFSFileSystem(is));
	}

	public void close() throws IOException {
		fs.close();
	}

	public String tryAll(File wordfile) throws IOException, GeneralSecurityException {
		BufferedReader r = new BufferedReader(new FileReader(wordfile));
		long start = System.currentTimeMillis();
		int count = 0;
		String valid = null;
		String password;
		while ((password = r.readLine()) != null) {
			if (isValid(password)) {
				valid = password;
				break;
			}
			count++;
			if ((count % 1000) == 0) {
				int secs = ((int) (((System.currentTimeMillis()) - start) / 1000));
				System.out.println(((((("Done " + count) + " passwords, ") + secs) + " seconds, last password ") + password));
			}
		} 
		r.close();
		return valid;
	}

	public boolean isValid(String password) throws GeneralSecurityException {
		return d.verifyPassword(password);
	}

	public static void main(String[] args) throws Exception {
		if ((args.length) < 2) {
			System.err.println("Use:");
			System.err.println("  OOXMLPasswordsTry <file.ooxml> <wordlist>");
			System.exit(1);
		}
		File ooxml = new File(args[0]);
		File words = new File(args[1]);
		System.out.println(((("Trying passwords from " + words) + " against ") + ooxml));
		System.out.println();
		OOXMLPasswordsTry pt = new OOXMLPasswordsTry(ooxml);
		String password = pt.tryAll(words);
		pt.close();
		System.out.println();
		if (password == null) {
			System.out.println("Error - No password matched");
		}else {
			System.out.println("Password found!");
			System.out.println(password);
		}
	}
}

