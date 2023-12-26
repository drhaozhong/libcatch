package org.apache.poi.hssf.usermodel.examples;


import java.io.FileInputStream;
import java.util.Iterator;
import java.util.List;
import org.apache.poi.hslf.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.SlideShow;
import org.apache.poi.hssf.usermodel.HSSFObjectData;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;


public class EmeddedObjects {
	public static void main(String[] args) throws Exception {
		POIFSFileSystem fs = new POIFSFileSystem(new FileInputStream(args[0]));
		HSSFWorkbook workbook = new HSSFWorkbook(fs);
		for (HSSFObjectData obj : workbook.getAllEmbeddedObjects()) {
			String oleName = obj.getOLE2ClassName();
			if (oleName.equals("Worksheet")) {
				DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
				HSSFWorkbook embeddedWorkbook = new HSSFWorkbook(dn, fs, false);
			}else
				if (oleName.equals("Document")) {
					DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
					HWPFDocument embeddedWordDocument = new HWPFDocument(dn, fs);
				}else
					if (oleName.equals("Presentation")) {
						DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
						SlideShow embeddedPowerPointDocument = new SlideShow(new HSLFSlideShow(dn, fs));
					}else {
						if (obj.hasDirectoryEntry()) {
							DirectoryNode dn = ((DirectoryNode) (obj.getDirectory()));
							for (Iterator entries = dn.getEntries(); entries.hasNext();) {
								Entry entry = ((Entry) (entries.next()));
							}
						}else {
							byte[] objectData = obj.getObjectData();
						}
					}


		}
	}
}

