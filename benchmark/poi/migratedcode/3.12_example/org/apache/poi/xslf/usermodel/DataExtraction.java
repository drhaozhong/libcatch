package org.apache.poi.xslf.usermodel;


import java.awt.Dimension;
import java.awt.Rectangle;
import java.awt.geom.Rectangle2D;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.POIXMLDocumentPart;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.openxml4j.opc.PackagePartName;
import org.apache.poi.sl.usermodel.Shape;


public final class DataExtraction {
	public static void main(String[] args) throws Exception {
		if ((args.length) == 0) {
			System.out.println("Input file is required");
			return;
		}
		FileInputStream is = new FileInputStream(args[0]);
		XMLSlideShow ppt = new XMLSlideShow(is);
		is.close();
		List<PackagePart> embeds = ppt.getAllEmbedds();
		for (PackagePart p : embeds) {
			String type = p.getContentType();
			String name = p.getPartName().getName();
			InputStream pIs = p.getInputStream();
			pIs.close();
		}
		List<XSLFPictureData> images = getAllPictures();
		for (XSLFPictureData data : images) {
			PackagePart p = data.getPackagePart();
			String type = p.getContentType();
			String name = data.getFileName();
			InputStream pIs = p.getInputStream();
			pIs.close();
		}
		Dimension pageSize = ppt.getPageSize();
		for (XSLFSlide slide : ppt.getSlides()) {
			for (XSLFShape shape : slide) {
				Rectangle2D anchor = shape.getAnchor();
				if (shape instanceof XSLFTextShape) {
					XSLFTextShape txShape = ((XSLFTextShape) (shape));
					System.out.println(txShape.getText());
				}else
					if (shape instanceof XSLFPictureShape) {
						XSLFPictureShape pShape = ((XSLFPictureShape) (shape));
						XSLFPictureData pData = pShape.getPictureData();
						System.out.println(pData.getFileName());
					}else {
						System.out.println(("Process me: " + (shape.getClass())));
					}

			}
		}
	}
}

