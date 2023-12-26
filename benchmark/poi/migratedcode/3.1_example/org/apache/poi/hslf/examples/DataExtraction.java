package org.apache.poi.hslf.examples;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import org.apache.poi.hslf.model.OLEShape;
import org.apache.poi.hslf.model.Picture;
import org.apache.poi.hslf.model.Shape;
import org.apache.poi.hslf.model.Sheet;
import org.apache.poi.hslf.model.Slide;
import org.apache.poi.hslf.usermodel.ObjectData;
import org.apache.poi.hslf.usermodel.PictureData;
import org.apache.poi.hslf.usermodel.SlideShow;
import org.apache.poi.hslf.usermodel.SoundData;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.usermodel.Paragraph;
import org.apache.poi.hwpf.usermodel.Range;


public class DataExtraction {
	public static void main(String[] args) throws Exception {
		if ((args.length) == 0) {
			DataExtraction.usage();
			return;
		}
		FileInputStream is = new FileInputStream(args[0]);
		SlideShow ppt = new SlideShow(is);
		is.close();
		SoundData[] sound = ppt.getSoundData();
		for (int i = 0; i < (sound.length); i++) {
			String type = sound[i].getSoundType();
			String name = sound[i].getSoundName();
			byte[] data = sound[i].getData();
			FileOutputStream out = new FileOutputStream((name + type));
			out.write(data);
			out.close();
		}
		Slide[] slide = ppt.getSlides();
		for (int i = 0; i < (slide.length); i++) {
			Shape[] shape = slide[i].getShapes();
			for (int j = 0; j < (shape.length); j++) {
				if ((shape[j]) instanceof OLEShape) {
					OLEShape ole = ((OLEShape) (shape[j]));
					ObjectData data = ole.getObjectData();
					String name = ole.getInstanceName();
					if ("Worksheet".equals(name)) {
						HSSFWorkbook wb = new HSSFWorkbook(data.getData());
					}else
						if ("Document".equals(name)) {
							HWPFDocument doc = new HWPFDocument(data.getData());
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
							InputStream dis = data.getData();
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
			Shape[] shape = slide[i].getShapes();
			for (int j = 0; j < (shape.length); j++) {
				if ((shape[j]) instanceof Picture) {
					Picture p = ((Picture) (shape[j]));
					PictureData data = p.getPictureData();
					String name = p.getPictureName();
					int type = data.getType();
					String ext;
					switch (type) {
						case Picture.JPEG :
							ext = ".jpg";
							break;
						case Picture.PNG :
							ext = ".png";
							break;
						case Picture.WMF :
							ext = ".wmf";
							break;
						case Picture.EMF :
							ext = ".emf";
							break;
						case Picture.PICT :
							ext = ".pict";
							break;
						case Picture.DIB :
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

