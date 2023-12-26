package org.apache.poi.crypt.examples;


import java.io.InputStream;
import org.apache.poi.poifs.crypt.Decryptor;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.util.IOUtils;


public class EncryptionUtils {
	private EncryptionUtils() {
	}

	public static InputStream decrypt(final InputStream inputStream, final String pwd) throws Exception {
		try {
			POIFSFileSystem fs = new POIFSFileSystem(inputStream);
			EncryptionInfo info = new EncryptionInfo(fs);
			Decryptor d = Decryptor.getInstance(info);
			if (!(d.verifyPassword(pwd))) {
				throw new RuntimeException("incorrect password");
			}
			return d.getDataStream(fs);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
	}
}

