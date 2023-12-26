package org.apache.poi.hssf.usermodel.examples;


import java.io.Closeable;
import java.io.FileInputStream;
import java.util.List;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hssf.usermodel.HSSFObjectData;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;


public class EmbeddedObjects {
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		POIFSFileSystem fs = new POIFSFileSystem(new FileInputStream(args[0]));
		try (HSSFWorkbook workbook = new HSSFWorkbook(fs)) {
			for (HSSFObjectData obj : workbook.getAllEmbeddedObjects()) {
				String oleName = obj.getOLE2ClassName();
				DirectoryNode dn = (obj.hasDirectoryEntry()) ? ((DirectoryNode) (obj.getDirectory())) : null;
				Closeable document = null;
				if (oleName.equals("Worksheet")) {
					document = new HSSFWorkbook(dn, fs, false);
				}else
					if (oleName.equals("Document")) {
						document = new HWPFDocument(dn);
					}else
						if (oleName.equals("Presentation")) {
							document = new HSLFSlideShow(dn);
						}else {
							if (dn != null) {
								for (Entry entry : dn) {
									String name = entry.getName();
								}
							}else {
								byte[] objectData = obj.getObjectData();
							}
						}


				if (document != null) {
					document.close();
				}
			}
		}
	}
}

