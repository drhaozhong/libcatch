package org.apache.poi.ss.examples;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.util.ArrayList;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;


public class ToCSV {
	private Workbook workbook;

	private ArrayList<ArrayList<String>> csvData;

	private int maxRowWidth;

	private int formattingConvention;

	private DataFormatter formatter;

	private FormulaEvaluator evaluator;

	private String separator;

	private static final String CSV_FILE_EXTENSION = ".csv";

	private static final String DEFAULT_SEPARATOR = ",";

	public static final int EXCEL_STYLE_ESCAPING = 0;

	public static final int UNIX_STYLE_ESCAPING = 1;

	public void convertExcelToCSV(String strSource, String strDestination) throws FileNotFoundException, IOException, IllegalArgumentException {
		this.convertExcelToCSV(strSource, strDestination, ToCSV.DEFAULT_SEPARATOR, ToCSV.EXCEL_STYLE_ESCAPING);
	}

	public void convertExcelToCSV(String strSource, String strDestination, String separator) throws FileNotFoundException, IOException, IllegalArgumentException {
		this.convertExcelToCSV(strSource, strDestination, separator, ToCSV.EXCEL_STYLE_ESCAPING);
	}

	public void convertExcelToCSV(String strSource, String strDestination, String separator, int formattingConvention) throws FileNotFoundException, IOException, IllegalArgumentException {
		File source = new File(strSource);
		File destination = new File(strDestination);
		File[] filesList;
		String destinationFilename;
		if (!(source.exists())) {
			throw new IllegalArgumentException(("The source for the Excel " + "file(s) cannot be found."));
		}
		if (!(destination.exists())) {
			throw new IllegalArgumentException(("The folder/directory for the " + "converted CSV file(s) does not exist."));
		}
		if (!(destination.isDirectory())) {
			throw new IllegalArgumentException(("The destination for the CSV " + "file(s) is not a directory/folder."));
		}
		if ((formattingConvention != (ToCSV.EXCEL_STYLE_ESCAPING)) && (formattingConvention != (ToCSV.UNIX_STYLE_ESCAPING))) {
			throw new IllegalArgumentException(("The value passed to the " + "formattingConvention parameter is out of range."));
		}
		this.separator = separator;
		this.formattingConvention = formattingConvention;
		if (source.isDirectory()) {
			filesList = source.listFiles(new ToCSV.ExcelFilenameFilter());
		}else {
			filesList = new File[]{ source };
		}
		if (filesList != null) {
			for (File excelFile : filesList) {
				this.openWorkbook(excelFile);
				this.convertToCSV();
				destinationFilename = excelFile.getName();
				destinationFilename = (destinationFilename.substring(0, destinationFilename.lastIndexOf("."))) + (ToCSV.CSV_FILE_EXTENSION);
				this.saveCSVFile(new File(destination, destinationFilename));
			}
		}
	}

	private void openWorkbook(File file) throws FileNotFoundException, IOException {
		System.out.println((("Opening workbook [" + (file.getName())) + "]"));
		try (FileInputStream fis = new FileInputStream(file)) {
			this.workbook = WorkbookFactory.create(fis);
			this.evaluator = this.workbook.getCreationHelper().createFormulaEvaluator();
			this.formatter = new DataFormatter(true);
		}
	}

	private void convertToCSV() {
		Sheet sheet;
		Row row;
		int lastRowNum;
		this.csvData = new ArrayList<>();
		System.out.println("Converting files contents to CSV format.");
		int numSheets = this.workbook.getNumberOfSheets();
		for (int i = 0; i < numSheets; i++) {
			sheet = this.workbook.getSheetAt(i);
			if ((sheet.getPhysicalNumberOfRows()) > 0) {
				lastRowNum = sheet.getLastRowNum();
				for (int j = 0; j <= lastRowNum; j++) {
					row = sheet.getRow(j);
					this.rowToCSV(row);
				}
			}
		}
	}

	private void saveCSVFile(File file) throws FileNotFoundException, IOException {
		ArrayList<String> line;
		StringBuffer buffer;
		String csvLineElement;
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
			System.out.println((("Saving the CSV file [" + (file.getName())) + "]"));
			for (int i = 0; i < (this.csvData.size()); i++) {
				buffer = new StringBuffer();
				line = this.csvData.get(i);
				for (int j = 0; j < (this.maxRowWidth); j++) {
					if ((line.size()) > j) {
						csvLineElement = line.get(j);
						if (csvLineElement != null) {
							buffer.append(this.escapeEmbeddedCharacters(csvLineElement));
						}
					}
					if (j < ((this.maxRowWidth) - 1)) {
						buffer.append(this.separator);
					}
				}
				bw.write(buffer.toString().trim());
				if (i < ((this.csvData.size()) - 1)) {
					bw.newLine();
				}
			}
		}
	}

	private void rowToCSV(Row row) {
		Cell cell;
		int lastCellNum;
		ArrayList<String> csvLine = new ArrayList<>();
		if (row != null) {
			lastCellNum = row.getLastCellNum();
			for (int i = 0; i <= lastCellNum; i++) {
				cell = row.getCell(i);
				if (cell == null) {
					csvLine.add("");
				}else {
					if ((cell.getCellType()) != (CellType.FORMULA)) {
						csvLine.add(this.formatter.formatCellValue(cell));
					}else {
						csvLine.add(this.formatter.formatCellValue(cell, this.evaluator));
					}
				}
			}
			if (lastCellNum > (this.maxRowWidth)) {
				this.maxRowWidth = lastCellNum;
			}
		}
		this.csvData.add(csvLine);
	}

	private String escapeEmbeddedCharacters(String field) {
		StringBuffer buffer;
		if ((this.formattingConvention) == (ToCSV.EXCEL_STYLE_ESCAPING)) {
			if (field.contains("\"")) {
				buffer = new StringBuffer(field.replaceAll("\"", "\\\"\\\""));
				buffer.insert(0, "\"");
				buffer.append("\"");
			}else {
				buffer = new StringBuffer(field);
				if (((buffer.indexOf(this.separator)) > (-1)) || ((buffer.indexOf("\n")) > (-1))) {
					buffer.insert(0, "\"");
					buffer.append("\"");
				}
			}
			return buffer.toString().trim();
		}else {
			if (field.contains(this.separator)) {
				field = field.replaceAll(this.separator, ("\\\\" + (this.separator)));
			}
			if (field.contains("\n")) {
				field = field.replaceAll("\n", "\\\\\n");
			}
			return field;
		}
	}

	public static void main(String[] args) {
		ToCSV converter;
		boolean converted = true;
		long startTime = System.currentTimeMillis();
		try {
			converter = new ToCSV();
			if ((args.length) == 2) {
				converter.convertExcelToCSV(args[0], args[1]);
			}else
				if ((args.length) == 3) {
					converter.convertExcelToCSV(args[0], args[1], args[2]);
				}else
					if ((args.length) == 4) {
						converter.convertExcelToCSV(args[0], args[1], args[2], Integer.parseInt(args[3]));
					}else {
						System.out.println(("Usage: java ToCSV [Source File/Folder] " + (((((((((((((((((("[Destination Folder] [Separator] [Formatting Convention]\n" + "\tSource File/Folder\tThis argument should contain the name of and\n") + "\t\t\t\tpath to either a single Excel workbook or a\n") + "\t\t\t\tfolder containing one or more Excel workbooks.\n") + "\tDestination Folder\tThe name of and path to the folder that the\n") + "\t\t\t\tCSV files should be written out into. The\n") + "\t\t\t\tfolder must exist before running the ToCSV\n") + "\t\t\t\tcode as it will not check for or create it.\n") + "\tSeparator\t\tOptional. The character or characters that\n") + "\t\t\t\tshould be used to separate fields in the CSV\n") + "\t\t\t\trecord. If no value is passed then the comma\n") + "\t\t\t\twill be assumed.\n") + "\tFormatting Convention\tOptional. This argument can take one of two\n") + "\t\t\t\tvalues. Passing 0 (zero) will result in a CSV\n") + "\t\t\t\tfile that obeys Excel\'s formatting conventions\n") + "\t\t\t\twhilst passing 1 (one) will result in a file\n") + "\t\t\t\tthat obeys UNIX formatting conventions. If no\n") + "\t\t\t\tvalue is passed, then the CSV file produced\n") + "\t\t\t\twill obey Excel\'s formatting conventions.")));
						converted = false;
					}


		} catch (Exception ex) {
			System.out.println(("Caught an: " + (ex.getClass().getName())));
			System.out.println(("Message: " + (ex.getMessage())));
			System.out.println("Stacktrace follows:.....");
			ex.printStackTrace(System.out);
			converted = false;
		}
		if (converted) {
			System.out.println((("Conversion took " + ((int) (((System.currentTimeMillis()) - startTime) / 1000))) + " seconds"));
		}
	}

	class ExcelFilenameFilter implements FilenameFilter {
		@Override
		public boolean accept(File file, String name) {
			return (name.endsWith(".xls")) || (name.endsWith(".xlsx"));
		}
	}
}

