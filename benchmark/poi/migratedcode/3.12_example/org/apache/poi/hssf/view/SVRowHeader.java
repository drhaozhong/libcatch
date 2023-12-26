package org.apache.poi.hssf.view;


import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import javax.swing.AbstractListModel;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JTable;
import javax.swing.ListCellRenderer;
import javax.swing.ListModel;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.table.JTableHeader;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;


public class SVRowHeader extends JList {
	private class SVRowHeaderModel extends AbstractListModel {
		private HSSFSheet sheet;

		public SVRowHeaderModel(HSSFSheet sheet) {
			this.sheet = sheet;
		}

		public int getSize() {
			return (sheet.getLastRowNum()) + 1;
		}

		public Object getElementAt(int index) {
			return Integer.toString((index + 1));
		}
	}

	private class RowHeaderRenderer extends JLabel implements ListCellRenderer {
		private HSSFSheet sheet;

		private int extraHeight;

		RowHeaderRenderer(HSSFSheet sheet, JTable table, int extraHeight) {
			this.sheet = sheet;
			this.extraHeight = extraHeight;
			JTableHeader header = table.getTableHeader();
			setOpaque(true);
			setBorder(UIManager.getBorder("TableHeader.cellBorder"));
			setHorizontalAlignment(SwingConstants.CENTER);
			setForeground(header.getForeground());
			setBackground(header.getBackground());
			setFont(header.getFont());
		}

		public Component getListCellRendererComponent(JList list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
			Dimension d = getPreferredSize();
			HSSFRow row = sheet.getRow(index);
			int rowHeight;
			if (row == null) {
				rowHeight = ((int) (sheet.getDefaultRowHeightInPoints()));
			}else {
				rowHeight = ((int) (row.getHeightInPoints()));
			}
			d.height = rowHeight + (extraHeight);
			setPreferredSize(d);
			setText((value == null ? "" : value.toString()));
			return this;
		}
	}

	public SVRowHeader(HSSFSheet sheet, JTable table, int extraHeight) {
		ListModel lm = new SVRowHeader.SVRowHeaderModel(sheet);
		this.setModel(lm);
		setFixedCellWidth(50);
		setCellRenderer(new SVRowHeader.RowHeaderRenderer(sheet, table, extraHeight));
	}
}

