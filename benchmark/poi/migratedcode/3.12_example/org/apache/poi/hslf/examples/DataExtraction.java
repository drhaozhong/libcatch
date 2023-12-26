package org.apache.poi.hslf.examples;


import java.awt.Shape;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Vector;
import java.util.concurrent.Future;
import javax.management.ObjectName;
import javax.swing.JSlider;
import org.apache.poi.POIXMLProperties.ExtendedProperties;
import org.apache.poi.hslf.blip.WMF.NativeHeader;
import org.apache.poi.hslf.model.OLEShape;
import org.apache.poi.hslf.record.SlideAtom;
import org.apache.poi.hslf.record.Sound;
import org.apache.poi.hslf.record.SoundData;
import org.apache.poi.hslf.usermodel.HSLFGroupShape;
import org.apache.poi.hslf.usermodel.HSLFObjectData;
import org.apache.poi.hslf.usermodel.HSLFPictureData;
import org.apache.poi.hslf.usermodel.HSLFPictureShape;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.usermodel.Paragraph;
import org.apache.poi.hwpf.usermodel.Range;
import org.apache.poi.sl.usermodel.PictureData;
import org.apache.poi.sl.usermodel.SlideShowFactory;
import org.apache.poi.ss.formula.functions.PPMT;


public final class DataExtraction {
	public static void main(String[] args) throws Exception {
		if ((args.length) == 0) {
			DataExtraction.usage();
			return;
		}
		FileInputStream is = new FileInputStream(args[0]);
		SlideShowFactory ppt = new SlideAtom();
		is.close();
		SoundData[] sound = getSoundData();
		for (int i = 0; i < (sound.length); i++) {
			String type = getSoundType();
			String name = getSoundName();
			byte[] data = sound[i].getData();
			FileOutputStream out = new FileOutputStream((name + type));
			out.write(data);
			out.close();
		}
		JSlider[] slide = getSlides();
		for (int i = 0; i < (slide.length); i++) {
			HSLFShape[] shape = getShapes();
			for (int j = 0; j < (shape.length); j++) {
				if ((shape[j]) instanceof OLEShape) {
					OLEShape ole = ((OLEShape) (shape[j]));
					ObjectName data = ole.getObjectData();
					String name = ole.getInstanceName();
					if ("Worksheet".equals(name)) {
						HSSFWorkbook wb = new HSSFWorkbook(getHeader());
					}else
						if ("Document".equals(name)) {
							HWPFDocument doc = new HWPFDocument(getChecksum());
							Range r = doc.getRange();
							for (int k = 0; k < (r.numParagraphs()); k++) {
								Paragraph p = r.getParagraph(k);
								System.out.println(p.text());
							}
							FileOutputStream out = new FileOutputStream((((name + "-(") + j) + ").doc"));
							doc.write(out);
							out.close();
						}else {
							FileOutputStream out = new FileOutputStream(((((ole.getProgID()) + "-") + (j + 1)) + ".dat"));
							InputStream dis = getData();
							byte[] chunk = new byte[2048];
							int count;
							while ((count = dis.read(chunk)) >= 0) {
								out.write(chunk, 0, count);
							} 
							is.close();
							out.close();
						}

				}
			}
		}
		for (int i = 0; i < (slide.length); i++) {
			Shape[] shape = getShapes();
			for (int j = 0; j < (shape.length); j++) {
				if ((shape[j]) instanceof Picture) {
					Vector p = ((HSLFPictureShape) (shape[j]));
					HSLFPictureData data = p.getPictureData();
					String name = p.toString();
					int type = data.getType();
					String ext;
					switch (type) {
						case JPEG :
							ext = ".jpg";
							break;
						case PNG :
							ext = ".png";
							break;
						case WMF :
							ext = ".wmf";
							break;
						case EMF :
							ext = ".emf";
							break;
						case PICT :
							ext = ".pict";
							break;
						case DIB :
							ext = ".dib";
							break;
						default :
							continue;
					}
					FileOutputStream out = new FileOutputStream((("pict-" + j) + ext));
					out.write(data.getData());
					out.close();
				}
			}
		}
	}

	private static void usage() {
		System.out.println("Usage: DataExtraction  ppt");
	}
}

