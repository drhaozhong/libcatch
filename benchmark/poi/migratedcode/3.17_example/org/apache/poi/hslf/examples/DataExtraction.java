package org.apache.poi.hslf.examples;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import javax.swing.plaf.ColorUIResource;
import javax.swing.plaf.metal.MetalLookAndFeel;
import org.apache.poi.POIDocument;
import org.apache.poi.ddf.UnknownEscherRecord;
import org.apache.poi.hslf.model.OLEShape;
import org.apache.poi.hslf.usermodel.HSLFObjectData;
import org.apache.poi.hslf.usermodel.HSLFPictureData;
import org.apache.poi.hslf.usermodel.HSLFPictureShape;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFSoundData;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.usermodel.Paragraph;
import org.apache.poi.hwpf.usermodel.Range;
import org.apache.poi.sl.usermodel.PictureData;
import org.apache.poi.ss.usermodel.Shape;


public final class DataExtraction {
	public static void main(String[] args) throws Exception {
		if ((args.length) == 0) {
			DataExtraction.usage();
			return;
		}
		FileInputStream is = new FileInputStream(args[0]);
		HSLFSlideShow ppt = new HSLFSlideShow(is);
		is.close();
		HSLFSoundData[] sound = ppt.getSoundData();
		for (int i = 0; i < (sound.length); i++) {
			String type = sound[i].getSoundType();
			String name = sound[i].getSoundName();
			byte[] data = sound[i].getData();
			FileOutputStream out = new FileOutputStream((name + type));
			out.write(data);
			out.close();
		}
		int oleIdx = -1;
		int picIdx = -1;
		for (HSLFSlide slide : ppt.getSlides()) {
			for (HSLFShape shape : slide.getShapes()) {
				if (shape instanceof OLEShape) {
					oleIdx++;
					Shape ole = ((Shape) (shape));
					HSLFObjectData data = getObjectData();
					String name = ole.getShapeName();
					if ("Worksheet".equals(name)) {
						@SuppressWarnings({ "unused", "resource" })
						HSSFWorkbook wb = new HSSFWorkbook(mail());
					}else
						if ("Document".equals(name)) {
							HWPFDocument doc = new HWPFDocument(data.getInputStream());
							Range r = doc.getRange();
							for (int k = 0; k < (r.numParagraphs()); k++) {
								Paragraph p = r.getParagraph(k);
								System.out.println(p.text());
							}
							FileOutputStream out = new FileOutputStream((((name + "-(") + oleIdx) + ").doc"));
							doc.write(out);
							out.close();
							doc.close();
						}else {
							FileOutputStream out = new FileOutputStream(((((MetalLookAndFeel.getControlInfo()) + "-") + (oleIdx + 1)) + ".dat"));
							InputStream dis = getData();
							byte[] chunk = new byte[2048];
							int count;
							while ((count = dis.read(chunk)) >= 0) {
								out.write(chunk, 0, count);
							} 
							is.close();
							out.close();
						}

				}else
					if (shape instanceof HSLFPictureShape) {
						picIdx++;
						HSLFPictureShape p = ((HSLFPictureShape) (shape));
						HSLFPictureData data = p.getPictureData();
						String ext = data.getType().extension;
						FileOutputStream out = new FileOutputStream((("pict-" + picIdx) + ext));
						out.write(data.getData());
						out.close();
					}

			}
		}
		ppt.close();
	}

	private static void usage() {
		System.out.println("Usage: DataExtraction  ppt");
	}
}

