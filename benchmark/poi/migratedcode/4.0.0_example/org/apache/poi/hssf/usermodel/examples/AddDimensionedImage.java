package org.apache.poi.hssf.usermodel.examples;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.poi.hssf.usermodel.HSSFClientAnchor;
import org.apache.poi.hssf.usermodel.HSSFPatriarch;
import org.apache.poi.hssf.usermodel.HSSFPicture;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.util.CellReference;

import static org.apache.poi.ss.usermodel.ClientAnchor.AnchorType.MOVE_AND_RESIZE;


public class AddDimensionedImage {
	public static final int EXPAND_ROW = 1;

	public static final int EXPAND_COLUMN = 2;

	public static final int EXPAND_ROW_AND_COLUMN = 3;

	public static final int OVERLAY_ROW_AND_COLUMN = 7;

	public void addImageToSheet(String cellNumber, HSSFSheet sheet, String imageFile, double reqImageWidthMM, double reqImageHeightMM, int resizeBehaviour) throws IOException, IllegalArgumentException {
		CellReference cellRef = new CellReference(cellNumber);
		this.addImageToSheet(cellRef.getCol(), cellRef.getRow(), sheet, imageFile, reqImageWidthMM, reqImageHeightMM, resizeBehaviour);
	}

	private void addImageToSheet(int colNumber, int rowNumber, HSSFSheet sheet, String imageFile, double reqImageWidthMM, double reqImageHeightMM, int resizeBehaviour) throws FileNotFoundException, IOException, IllegalArgumentException {
		HSSFClientAnchor anchor;
		HSSFPatriarch patriarch;
		AddDimensionedImage.ClientAnchorDetail rowClientAnchorDetail;
		AddDimensionedImage.ClientAnchorDetail colClientAnchorDetail;
		if ((((resizeBehaviour != (AddDimensionedImage.EXPAND_COLUMN)) && (resizeBehaviour != (AddDimensionedImage.EXPAND_ROW))) && (resizeBehaviour != (AddDimensionedImage.EXPAND_ROW_AND_COLUMN))) && (resizeBehaviour != (AddDimensionedImage.OVERLAY_ROW_AND_COLUMN))) {
			throw new IllegalArgumentException(("Invalid value passed to the " + "resizeBehaviour parameter of AddDimensionedImage.addImageToSheet()"));
		}
		colClientAnchorDetail = this.fitImageToColumns(sheet, colNumber, reqImageWidthMM, resizeBehaviour);
		rowClientAnchorDetail = this.fitImageToRows(sheet, rowNumber, reqImageHeightMM, resizeBehaviour);
		int dx1 = 0;
		int dy1 = 0;
		int dx2 = 0;
		int dy2 = 0;
		short col1 = 0;
		short col2 = 0;
		short row1 = 0;
		short row2 = 0;
		if (colClientAnchorDetail != null) {
			dx2 = colClientAnchorDetail.getInset();
			col1 = ((short) (colClientAnchorDetail.getFromIndex()));
			col2 = ((short) (colClientAnchorDetail.getToIndex()));
		}
		if (rowClientAnchorDetail != null) {
			dy2 = rowClientAnchorDetail.getInset();
			row1 = ((short) (rowClientAnchorDetail.getFromIndex()));
			row2 = ((short) (rowClientAnchorDetail.getToIndex()));
		}
		anchor = new HSSFClientAnchor(dx1, dy1, dx2, dy2, col1, row1, col2, row2);
		anchor.setAnchorType(MOVE_AND_RESIZE);
		int index = sheet.getWorkbook().addPicture(this.imageToBytes(imageFile), HSSFWorkbook.PICTURE_TYPE_PNG);
		patriarch = sheet.createDrawingPatriarch();
		patriarch.createPicture(anchor, index);
	}

	private AddDimensionedImage.ClientAnchorDetail fitImageToColumns(HSSFSheet sheet, int colNumber, double reqImageWidthMM, int resizeBehaviour) {
		double colWidthMM;
		double colCoordinatesPerMM;
		int pictureWidthCoordinates;
		AddDimensionedImage.ClientAnchorDetail colClientAnchorDetail = null;
		colWidthMM = AddDimensionedImage.ConvertImageUnits.widthUnits2Millimetres(((short) (sheet.getColumnWidth(colNumber))));
		if (colWidthMM < reqImageWidthMM) {
			if ((resizeBehaviour == (AddDimensionedImage.EXPAND_COLUMN)) || (resizeBehaviour == (AddDimensionedImage.EXPAND_ROW_AND_COLUMN))) {
				sheet.setColumnWidth(colNumber, AddDimensionedImage.ConvertImageUnits.millimetres2WidthUnits(reqImageWidthMM));
				colWidthMM = reqImageWidthMM;
				colCoordinatesPerMM = (AddDimensionedImage.ConvertImageUnits.TOTAL_COLUMN_COORDINATE_POSITIONS) / colWidthMM;
				pictureWidthCoordinates = ((int) (reqImageWidthMM * colCoordinatesPerMM));
				colClientAnchorDetail = new AddDimensionedImage.ClientAnchorDetail(colNumber, colNumber, pictureWidthCoordinates);
			}else
				if ((resizeBehaviour == (AddDimensionedImage.OVERLAY_ROW_AND_COLUMN)) || (resizeBehaviour == (AddDimensionedImage.EXPAND_ROW))) {
					colClientAnchorDetail = this.calculateColumnLocation(sheet, colNumber, reqImageWidthMM);
				}

		}else {
			colCoordinatesPerMM = (AddDimensionedImage.ConvertImageUnits.TOTAL_COLUMN_COORDINATE_POSITIONS) / colWidthMM;
			pictureWidthCoordinates = ((int) (reqImageWidthMM * colCoordinatesPerMM));
			colClientAnchorDetail = new AddDimensionedImage.ClientAnchorDetail(colNumber, colNumber, pictureWidthCoordinates);
		}
		return colClientAnchorDetail;
	}

	private AddDimensionedImage.ClientAnchorDetail fitImageToRows(HSSFSheet sheet, int rowNumber, double reqImageHeightMM, int resizeBehaviour) {
		double rowCoordinatesPerMM;
		int pictureHeightCoordinates;
		AddDimensionedImage.ClientAnchorDetail rowClientAnchorDetail = null;
		HSSFRow row = sheet.getRow(rowNumber);
		if (row == null) {
			row = sheet.createRow(rowNumber);
		}
		double rowHeightMM = (row.getHeightInPoints()) / (AddDimensionedImage.ConvertImageUnits.POINTS_PER_MILLIMETRE);
		if (rowHeightMM < reqImageHeightMM) {
			if ((resizeBehaviour == (AddDimensionedImage.EXPAND_ROW)) || (resizeBehaviour == (AddDimensionedImage.EXPAND_ROW_AND_COLUMN))) {
				row.setHeightInPoints(((float) (reqImageHeightMM * (AddDimensionedImage.ConvertImageUnits.POINTS_PER_MILLIMETRE))));
				rowHeightMM = reqImageHeightMM;
				rowCoordinatesPerMM = (AddDimensionedImage.ConvertImageUnits.TOTAL_ROW_COORDINATE_POSITIONS) / rowHeightMM;
				pictureHeightCoordinates = ((int) (reqImageHeightMM * rowCoordinatesPerMM));
				rowClientAnchorDetail = new AddDimensionedImage.ClientAnchorDetail(rowNumber, rowNumber, pictureHeightCoordinates);
			}else
				if ((resizeBehaviour == (AddDimensionedImage.OVERLAY_ROW_AND_COLUMN)) || (resizeBehaviour == (AddDimensionedImage.EXPAND_COLUMN))) {
					rowClientAnchorDetail = this.calculateRowLocation(sheet, rowNumber, reqImageHeightMM);
				}

		}else {
			rowCoordinatesPerMM = (AddDimensionedImage.ConvertImageUnits.TOTAL_ROW_COORDINATE_POSITIONS) / rowHeightMM;
			pictureHeightCoordinates = ((int) (reqImageHeightMM * rowCoordinatesPerMM));
			rowClientAnchorDetail = new AddDimensionedImage.ClientAnchorDetail(rowNumber, rowNumber, pictureHeightCoordinates);
		}
		return rowClientAnchorDetail;
	}

	private AddDimensionedImage.ClientAnchorDetail calculateColumnLocation(HSSFSheet sheet, int startingColumn, double reqImageWidthMM) {
		AddDimensionedImage.ClientAnchorDetail anchorDetail;
		double totalWidthMM = 0.0;
		double colWidthMM = 0.0;
		double overlapMM;
		double coordinatePositionsPerMM;
		int toColumn = startingColumn;
		int inset;
		while (totalWidthMM < reqImageWidthMM) {
			colWidthMM = AddDimensionedImage.ConvertImageUnits.widthUnits2Millimetres(((short) (sheet.getColumnWidth(toColumn))));
			totalWidthMM += colWidthMM + (AddDimensionedImage.ConvertImageUnits.CELL_BORDER_WIDTH_MILLIMETRES);
			toColumn++;
		} 
		toColumn--;
		if (((int) (totalWidthMM)) == ((int) (reqImageWidthMM))) {
			anchorDetail = new AddDimensionedImage.ClientAnchorDetail(startingColumn, toColumn, AddDimensionedImage.ConvertImageUnits.TOTAL_COLUMN_COORDINATE_POSITIONS);
		}else {
			overlapMM = reqImageWidthMM - (totalWidthMM - colWidthMM);
			if (overlapMM < 0) {
				overlapMM = 0.0;
			}
			coordinatePositionsPerMM = (AddDimensionedImage.ConvertImageUnits.TOTAL_COLUMN_COORDINATE_POSITIONS) / colWidthMM;
			inset = ((int) (coordinatePositionsPerMM * overlapMM));
			anchorDetail = new AddDimensionedImage.ClientAnchorDetail(startingColumn, toColumn, inset);
		}
		return anchorDetail;
	}

	private AddDimensionedImage.ClientAnchorDetail calculateRowLocation(HSSFSheet sheet, int startingRow, double reqImageHeightMM) {
		AddDimensionedImage.ClientAnchorDetail clientAnchorDetail;
		HSSFRow row;
		double rowHeightMM = 0.0;
		double totalRowHeightMM = 0.0;
		double overlapMM;
		double rowCoordinatesPerMM;
		int toRow = startingRow;
		int inset;
		while (totalRowHeightMM < reqImageHeightMM) {
			row = sheet.getRow(toRow);
			if (row == null) {
				row = sheet.createRow(toRow);
			}
			rowHeightMM = (row.getHeightInPoints()) / (AddDimensionedImage.ConvertImageUnits.POINTS_PER_MILLIMETRE);
			totalRowHeightMM += rowHeightMM;
			toRow++;
		} 
		toRow--;
		if (((int) (totalRowHeightMM)) == ((int) (reqImageHeightMM))) {
			clientAnchorDetail = new AddDimensionedImage.ClientAnchorDetail(startingRow, toRow, AddDimensionedImage.ConvertImageUnits.TOTAL_ROW_COORDINATE_POSITIONS);
		}else {
			overlapMM = reqImageHeightMM - (totalRowHeightMM - rowHeightMM);
			if (overlapMM < 0) {
				overlapMM = 0.0;
			}
			rowCoordinatesPerMM = (AddDimensionedImage.ConvertImageUnits.TOTAL_ROW_COORDINATE_POSITIONS) / rowHeightMM;
			inset = ((int) (overlapMM * rowCoordinatesPerMM));
			clientAnchorDetail = new AddDimensionedImage.ClientAnchorDetail(startingRow, toRow, inset);
		}
		return clientAnchorDetail;
	}

	private byte[] imageToBytes(String imageFilename) throws IOException {
		File imageFile;
		FileInputStream fis = null;
		ByteArrayOutputStream bos;
		int read;
		try {
			imageFile = new File(imageFilename);
			fis = new FileInputStream(imageFile);
			bos = new ByteArrayOutputStream();
			while ((read = fis.read()) != (-1)) {
				bos.write(read);
			} 
			return bos.toByteArray();
		} finally {
			if (fis != null) {
				try {
					fis.close();
					fis = null;
				} catch (IOException ioEx) {
				}
			}
		}
	}

	public static void main(String[] args) {
		String imageFile;
		String outputFile;
		FileOutputStream fos = null;
		HSSFSheet sheet;
		try {
			if ((args.length) < 2) {
				System.err.println("Usage: AddDimensionedImage imageFile outputFile");
				return;
			}
			imageFile = args[0];
			outputFile = args[1];
			try (HSSFWorkbook workbook = new HSSFWorkbook()) {
				sheet = workbook.createSheet("Picture Test");
				new AddDimensionedImage().addImageToSheet("A1", sheet, imageFile, 125, 125, AddDimensionedImage.EXPAND_ROW_AND_COLUMN);
				fos = new FileOutputStream(outputFile);
				workbook.write(fos);
			}
		} catch (IOException ioEx) {
			System.out.println(("Caught an: " + (ioEx.getClass().getName())));
			System.out.println(("Message: " + (ioEx.getMessage())));
			System.out.println("Stacktrace follows...........");
			ioEx.printStackTrace(System.out);
		} finally {
			try {
				if (fos != null) {
					fos.close();
					fos = null;
				}
			} catch (IOException ioEx) {
			}
		}
	}

	public class ClientAnchorDetail {
		public int fromIndex;

		public int toIndex;

		public int inset;

		public ClientAnchorDetail(int fromIndex, int toIndex, int inset) {
			this.fromIndex = fromIndex;
			this.toIndex = toIndex;
			this.inset = inset;
		}

		public int getFromIndex() {
			return this.fromIndex;
		}

		public int getToIndex() {
			return this.toIndex;
		}

		public int getInset() {
			return this.inset;
		}
	}

	public static class ConvertImageUnits {
		public static final int TOTAL_COLUMN_COORDINATE_POSITIONS = 1023;

		public static final int TOTAL_ROW_COORDINATE_POSITIONS = 255;

		public static final int PIXELS_PER_INCH = 96;

		public static final double PIXELS_PER_MILLIMETRES = 3.78;

		public static final double POINTS_PER_MILLIMETRE = 2.83;

		public static final double CELL_BORDER_WIDTH_MILLIMETRES = 2.0;

		public static final short EXCEL_COLUMN_WIDTH_FACTOR = 256;

		public static final int UNIT_OFFSET_LENGTH = 7;

		public static final int[] UNIT_OFFSET_MAP = new int[]{ 0, 36, 73, 109, 146, 182, 219 };

		public static short pixel2WidthUnits(int pxs) {
			short widthUnits = ((short) ((AddDimensionedImage.ConvertImageUnits.EXCEL_COLUMN_WIDTH_FACTOR) * (pxs / (AddDimensionedImage.ConvertImageUnits.UNIT_OFFSET_LENGTH))));
			widthUnits += AddDimensionedImage.ConvertImageUnits.UNIT_OFFSET_MAP[(pxs % (AddDimensionedImage.ConvertImageUnits.UNIT_OFFSET_LENGTH))];
			return widthUnits;
		}

		public static int widthUnits2Pixel(short widthUnits) {
			int pixels = (widthUnits / (AddDimensionedImage.ConvertImageUnits.EXCEL_COLUMN_WIDTH_FACTOR)) * (AddDimensionedImage.ConvertImageUnits.UNIT_OFFSET_LENGTH);
			int offsetWidthUnits = widthUnits % (AddDimensionedImage.ConvertImageUnits.EXCEL_COLUMN_WIDTH_FACTOR);
			pixels += Math.round((offsetWidthUnits / (((float) (AddDimensionedImage.ConvertImageUnits.EXCEL_COLUMN_WIDTH_FACTOR)) / (AddDimensionedImage.ConvertImageUnits.UNIT_OFFSET_LENGTH))));
			return pixels;
		}

		public static double widthUnits2Millimetres(short widthUnits) {
			return (AddDimensionedImage.ConvertImageUnits.widthUnits2Pixel(widthUnits)) / (AddDimensionedImage.ConvertImageUnits.PIXELS_PER_MILLIMETRES);
		}

		public static int millimetres2WidthUnits(double millimetres) {
			return AddDimensionedImage.ConvertImageUnits.pixel2WidthUnits(((int) (millimetres * (AddDimensionedImage.ConvertImageUnits.PIXELS_PER_MILLIMETRES))));
		}
	}
}

