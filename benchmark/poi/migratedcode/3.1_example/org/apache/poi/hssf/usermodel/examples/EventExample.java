package org.apache.poi.hssf.usermodel.examples;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RowRecord;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.UnicodeString;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;


public class EventExample implements HSSFListener {
	private SSTRecord sstrec;

	public void processRecord(Record record) {
		switch (record.getSid()) {
			case BOFRecord.sid :
				BOFRecord bof = ((BOFRecord) (record));
				if ((bof.getType()) == (bof.TYPE_WORKBOOK)) {
					System.out.println("Encountered workbook");
				}else
					if ((bof.getType()) == (bof.TYPE_WORKSHEET)) {
						System.out.println("Encountered sheet reference");
					}

				break;
			case BoundSheetRecord.sid :
				BoundSheetRecord bsr = ((BoundSheetRecord) (record));
				System.out.println(("New sheet named: " + (bsr.getSheetname())));
				break;
			case RowRecord.sid :
				RowRecord rowrec = ((RowRecord) (record));
				System.out.println(((("Row found, first column at " + (rowrec.getFirstCol())) + " last column at ") + (rowrec.getLastCol())));
				break;
			case NumberRecord.sid :
				NumberRecord numrec = ((NumberRecord) (record));
				System.out.println(((((("Cell found with value " + (numrec.getValue())) + " at row ") + (numrec.getRow())) + " and column ") + (numrec.getColumn())));
				break;
			case SSTRecord.sid :
				sstrec = ((SSTRecord) (record));
				for (int k = 0; k < (sstrec.getNumUniqueStrings()); k++) {
					System.out.println(((("String table value " + k) + " = ") + (sstrec.getString(k))));
				}
				break;
			case LabelSSTRecord.sid :
				LabelSSTRecord lrec = ((LabelSSTRecord) (record));
				System.out.println(("String cell found with value " + (sstrec.getString(lrec.getSSTIndex()))));
				break;
		}
	}

	public static void main(String[] args) throws IOException {
		FileInputStream fin = new FileInputStream(args[0]);
		POIFSFileSystem poifs = new POIFSFileSystem(fin);
		InputStream din = poifs.createDocumentInputStream("Workbook");
		HSSFRequest req = new HSSFRequest();
		req.addListenerForAllRecords(new EventExample());
		HSSFEventFactory factory = new HSSFEventFactory();
		factory.processEvents(req, din);
		fin.close();
		din.close();
		System.out.println("done.");
	}
}

