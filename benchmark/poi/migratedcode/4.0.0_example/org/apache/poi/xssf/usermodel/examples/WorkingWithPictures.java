package org.apache.poi.xssf.usermodel.examples;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class WorkingWithPictures {
	public static void main(String[] args) throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			CreationHelper helper = wb.getCreationHelper();
			InputStream is = new FileInputStream(args[0]);
			byte[] bytes = IOUtils.toByteArray(is);
			is.close();
			int pictureIdx = wb.addPicture(bytes, Workbook.PICTURE_TYPE_JPEG);
			Sheet sheet = wb.createSheet();
			Drawing<?> drawing = sheet.createDrawingPatriarch();
			ClientAnchor anchor = helper.createClientAnchor();
			anchor.setCol1(1);
			anchor.setRow1(1);
			Picture pict = drawing.createPicture(anchor, pictureIdx);
			pict.resize(2);
			String file = "picture.xls";
			if (wb instanceof XSSFWorkbook) {
				file += "x";
			}
			try (OutputStream fileOut = new FileOutputStream(file)) {
				wb.write(fileOut);
			}
		}
	}
}

