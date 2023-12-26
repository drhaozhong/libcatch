package org.apache.poi.hssf.view;


import java.io.PrintStream;
import java.util.Iterator;
import javax.swing.table.AbstractTableModel;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Row;


public class SVTableModel extends AbstractTableModel {
	private HSSFSheet st = null;

	int maxcol = 0;

	public SVTableModel(HSSFSheet st, int maxcol) {
		this.st = st;
		this.maxcol = maxcol;
	}

	public SVTableModel(HSSFSheet st) {
		this.st = st;
		Iterator<Row> i = st.rowIterator();
		while (i.hasNext()) {
			HSSFRow row = ((HSSFRow) (i.next()));
			if ((maxcol) < ((row.getLastCellNum()) + 1)) {
				this.maxcol = row.getLastCellNum();
			}
		} 
	}

	public int getColumnCount() {
		return (this.maxcol) + 1;
	}

	public Object getValueAt(int row, int col) {
		HSSFRow r = st.getRow(row);
		HSSFCell c = null;
		if (r != null) {
			c = r.getCell(col);
		}
		return c;
	}

	public int getRowCount() {
		return (st.getLastRowNum()) + 1;
	}

	public Class<?> getColumnClass(int c) {
		return HSSFCell.class;
	}

	public boolean isCellEditable(int rowIndex, int columnIndex) {
		return true;
	}

	public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
		if (aValue != null)
			System.out.println(("SVTableModel.setValueAt. value type = " + (aValue.getClass().getName())));
		else
			System.out.println("SVTableModel.setValueAt. value type = null");

	}
}

