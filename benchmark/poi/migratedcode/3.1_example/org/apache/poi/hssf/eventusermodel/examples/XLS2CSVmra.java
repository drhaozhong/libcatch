package org.apache.poi.hssf.eventusermodel.examples;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.poi.hssf.eventusermodel.FormatTrackingHSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.eventusermodel.MissingRecordAwareHSSFListener;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.record.BlankRecord;
import org.apache.poi.hssf.record.BoolErrRecord;
import org.apache.poi.hssf.record.CellValueRecordInterface;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.LabelRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NoteRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.RKRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.StringRecord;
import org.apache.poi.hssf.record.UnicodeString;
import org.apache.poi.hssf.record.formula.Ptg;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.formula.FormulaRenderer;


public class XLS2CSVmra implements HSSFListener {
	private int minColumns;

	private POIFSFileSystem fs;

	private PrintStream output;

	private int lastRowNumber;

	private int lastColumnNumber;

	private boolean outputFormulaValues = true;

	private SSTRecord sstRecord;

	private FormatTrackingHSSFListener formatListener;

	private int nextRow;

	private int nextColumn;

	private boolean outputNextStringRecord;

	public XLS2CSVmra(POIFSFileSystem fs, PrintStream output, int minColumns) {
		this.fs = fs;
		this.output = output;
		this.minColumns = minColumns;
	}

	public XLS2CSVmra(String filename, int minColumns) throws FileNotFoundException, IOException {
		this(new POIFSFileSystem(new FileInputStream(filename)), System.out, minColumns);
	}

	public void process() throws IOException {
		MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this);
		formatListener = new FormatTrackingHSSFListener(listener);
		HSSFEventFactory factory = new HSSFEventFactory();
		HSSFRequest request = new HSSFRequest();
		request.addListenerForAllRecords(formatListener);
		factory.processWorkbookEvents(request, fs);
	}

	public void processRecord(Record record) {
		int thisRow = -1;
		int thisColumn = -1;
		String thisStr = null;
		switch (record.getSid()) {
			case SSTRecord.sid :
				sstRecord = ((SSTRecord) (record));
				break;
			case BlankRecord.sid :
				BlankRecord brec = ((BlankRecord) (record));
				thisRow = brec.getRow();
				thisColumn = brec.getColumn();
				thisStr = "";
				break;
			case BoolErrRecord.sid :
				BoolErrRecord berec = ((BoolErrRecord) (record));
				thisRow = berec.getRow();
				thisColumn = berec.getColumn();
				thisStr = "";
				break;
			case FormulaRecord.sid :
				FormulaRecord frec = ((FormulaRecord) (record));
				thisRow = frec.getRow();
				thisColumn = frec.getColumn();
				if (outputFormulaValues) {
					if (Double.isNaN(frec.getValue())) {
						outputNextStringRecord = true;
						nextRow = frec.getRow();
						nextColumn = frec.getColumn();
					}else {
						thisStr = formatNumberDateCell(frec, frec.getValue());
					}
				}else {
					thisStr = ('"' + (FormulaRenderer.toFormulaString(null, frec.getParsedExpression()))) + '"';
				}
				break;
			case StringRecord.sid :
				if (outputNextStringRecord) {
					StringRecord srec = ((StringRecord) (record));
					thisStr = srec.getString();
					thisRow = nextRow;
					thisColumn = nextColumn;
					outputNextStringRecord = false;
				}
				break;
			case LabelRecord.sid :
				LabelRecord lrec = ((LabelRecord) (record));
				thisRow = lrec.getRow();
				thisColumn = lrec.getColumn();
				thisStr = ('"' + (lrec.getValue())) + '"';
				break;
			case LabelSSTRecord.sid :
				LabelSSTRecord lsrec = ((LabelSSTRecord) (record));
				thisRow = lsrec.getRow();
				thisColumn = lsrec.getColumn();
				if ((sstRecord) == null) {
					thisStr = ('"' + "(No SST Record, can't identify string)") + '"';
				}else {
					thisStr = ('"' + (sstRecord.getString(lsrec.getSSTIndex()).toString())) + '"';
				}
				break;
			case NoteRecord.sid :
				NoteRecord nrec = ((NoteRecord) (record));
				thisRow = nrec.getRow();
				thisColumn = nrec.getColumn();
				thisStr = ('"' + "(TODO)") + '"';
				break;
			case NumberRecord.sid :
				NumberRecord numrec = ((NumberRecord) (record));
				thisRow = numrec.getRow();
				thisColumn = numrec.getColumn();
				thisStr = formatNumberDateCell(numrec, numrec.getValue());
				break;
			case RKRecord.sid :
				RKRecord rkrec = ((RKRecord) (record));
				thisRow = rkrec.getRow();
				thisColumn = rkrec.getColumn();
				thisStr = ('"' + "(TODO)") + '"';
				break;
			default :
				break;
		}
		if ((thisRow != (-1)) && (thisRow != (lastRowNumber))) {
			lastColumnNumber = -1;
		}
		if (record instanceof MissingCellDummyRecord) {
			MissingCellDummyRecord mc = ((MissingCellDummyRecord) (record));
			thisRow = mc.getRow();
			thisColumn = mc.getColumn();
			thisStr = "";
		}
		if (thisStr != null) {
			if (thisColumn > 0) {
				output.print(',');
			}
			output.print(thisStr);
		}
		if (thisRow > (-1))
			lastRowNumber = thisRow;

		if (thisColumn > (-1))
			lastColumnNumber = thisColumn;

		if (record instanceof LastCellOfRowDummyRecord) {
			if ((minColumns) > 0) {
				if ((lastColumnNumber) == (-1)) {
					lastColumnNumber = 0;
				}
				for (int i = lastColumnNumber; i < (minColumns); i++) {
					output.print(',');
				}
			}
			lastColumnNumber = -1;
			output.println();
		}
	}

	private String formatNumberDateCell(CellValueRecordInterface cell, double value) {
		int formatIndex = formatListener.getFormatIndex(cell);
		String formatString = formatListener.getFormatString(cell);
		if (formatString == null) {
			return Double.toString(value);
		}else {
			if ((HSSFDateUtil.isADateFormat(formatIndex, formatString)) && (HSSFDateUtil.isValidExcelDate(value))) {
				formatString = formatString.replace('m', 'M');
				formatString = formatString.replaceAll("\\\\-", "-");
				Date d = HSSFDateUtil.getJavaDate(value, false);
				DateFormat df = new SimpleDateFormat(formatString);
				return df.format(d);
			}else {
				if (formatString == "General") {
					return Double.toString(value);
				}
				DecimalFormat df = new DecimalFormat(formatString);
				return df.format(value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if ((args.length) < 1) {
			System.err.println("Use:");
			System.err.println("  XLS2CSVmra <xls file> [min columns]");
			System.exit(1);
		}
		int minColumns = -1;
		if ((args.length) >= 2) {
			minColumns = Integer.parseInt(args[1]);
		}
		XLS2CSVmra xls2csv = new XLS2CSVmra(args[0], minColumns);
		xls2csv.process();
	}
}

