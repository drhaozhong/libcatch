package org.apache.poi.hssf.view;


import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Toolkit;
import java.awt.event.HierarchyEvent;
import java.awt.event.HierarchyListener;
import java.io.PrintStream;
import javax.swing.BorderFactory;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JViewport;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.border.Border;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import javax.swing.table.TableModel;
import javax.swing.text.JTextComponent;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.view.brush.PendingPaintings;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;


public class SVSheetTable extends JTable {
	private final HSSFSheet sheet;

	private final PendingPaintings pendingPaintings;

	private SVSheetTable.FormulaDisplayListener formulaListener;

	private JScrollPane scroll;

	private static final Color HEADER_BACKGROUND = new Color(235, 235, 235);

	private static final int magicCharFactor = 7;

	private class HeaderCell extends JLabel {
		private final int row;

		public HeaderCell(Object value, int row) {
			super(value.toString(), SwingConstants.CENTER);
			this.row = row;
			setBackground(SVSheetTable.HEADER_BACKGROUND);
			setOpaque(true);
			setBorder(BorderFactory.createLineBorder(Color.LIGHT_GRAY));
			setRowSelectionAllowed(false);
		}

		@Override
		public Dimension getPreferredSize() {
			Dimension d = super.getPreferredSize();
			if ((row) >= 0) {
				d.height = getRowHeight(row);
			}
			return d;
		}

		@Override
		public Dimension getMaximumSize() {
			Dimension d = super.getMaximumSize();
			if ((row) >= 0) {
				d.height = getRowHeight(row);
			}
			return d;
		}

		@Override
		public Dimension getMinimumSize() {
			Dimension d = super.getMinimumSize();
			if ((row) >= 0) {
				d.height = getRowHeight(row);
			}
			return d;
		}
	}

	private class HeaderCellRenderer implements TableCellRenderer {
		public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
			return new SVSheetTable.HeaderCell(value, row);
		}
	}

	private class FormulaDisplayListener implements ListSelectionListener {
		private final JTextComponent formulaDisplay;

		public FormulaDisplayListener(JTextComponent formulaDisplay) {
			this.formulaDisplay = formulaDisplay;
		}

		public void valueChanged(ListSelectionEvent e) {
			int row = getSelectedRow();
			int col = getSelectedColumn();
			if ((row < 0) || (col < 0)) {
				return;
			}
			if (e.getValueIsAdjusting()) {
				return;
			}
			HSSFCell cell = ((HSSFCell) (getValueAt(row, col)));
			String formula = "";
			if (cell != null) {
				if ((cell.getCellTypeEnum()) == (CellType.FORMULA)) {
					formula = cell.getCellFormula();
				}else {
					formula = cell.toString();
				}
				if (formula == null)
					formula = "";

			}
			formulaDisplay.setText(formula);
		}
	}

	public SVSheetTable(HSSFSheet sheet) {
		super(new SVTableModel(sheet));
		this.sheet = sheet;
		setIntercellSpacing(new Dimension(0, 0));
		setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		JTableHeader header = getTableHeader();
		header.setDefaultRenderer(new SVSheetTable.HeaderCellRenderer());
		pendingPaintings = new PendingPaintings(this);
		TableColumnModel columns = getColumnModel();
		for (int i = 0; i < (columns.getColumnCount()); i++) {
			TableColumn column = columns.getColumn(i);
			int width = sheet.getColumnWidth(i);
			column.setPreferredWidth(((width / 256) * (SVSheetTable.magicCharFactor)));
		}
		Toolkit t = getToolkit();
		int res = t.getScreenResolution();
		TableModel model = getModel();
		for (int i = 0; i < (model.getRowCount()); i++) {
			Row row = sheet.getRow((i - (sheet.getFirstRowNum())));
			if (row != null) {
				short h = row.getHeight();
				int height = ((int) (Math.round(Math.max(1.0, ((h / ((res / 70.0) * 20.0)) + 3.0)))));
				System.out.printf("%d: %d (%d @ %d)%n", i, height, h, res);
				setRowHeight(i, height);
			}
		}
		addHierarchyListener(new HierarchyListener() {
			public void hierarchyChanged(HierarchyEvent e) {
				if (((e.getChangeFlags()) & (HierarchyEvent.PARENT_CHANGED)) != 0) {
					Container changedParent = e.getChangedParent();
					if (changedParent instanceof JViewport) {
						Container grandparent = changedParent.getParent();
						if (grandparent instanceof JScrollPane) {
							JScrollPane jScrollPane = ((JScrollPane) (grandparent));
							setupScroll(jScrollPane);
						}
					}
				}
			}
		});
	}

	public void setupScroll(JScrollPane scroll) {
		if (scroll == (this.scroll))
			return;

		this.scroll = scroll;
		if (scroll == null)
			return;

		SVRowHeader rowHeader = new SVRowHeader(sheet, this, 0);
		scroll.setRowHeaderView(rowHeader);
		scroll.setCorner(JScrollPane.UPPER_LEADING_CORNER, headerCell("?"));
	}

	public void setFormulaDisplay(JTextComponent formulaDisplay) {
		ListSelectionModel rowSelMod = getSelectionModel();
		ListSelectionModel colSelMod = getColumnModel().getSelectionModel();
		if (formulaDisplay == null) {
			rowSelMod.removeListSelectionListener(formulaListener);
			colSelMod.removeListSelectionListener(formulaListener);
			formulaListener = null;
		}
		if (formulaDisplay != null) {
			formulaListener = new SVSheetTable.FormulaDisplayListener(formulaDisplay);
			rowSelMod.addListSelectionListener(formulaListener);
			colSelMod.addListSelectionListener(formulaListener);
		}
	}

	public JTextComponent getFormulaDisplay() {
		if ((formulaListener) == null)
			return null;
		else
			return formulaListener.formulaDisplay;

	}

	public Component headerCell(String text) {
		return new SVSheetTable.HeaderCell(text, (-1));
	}

	@Override
	public void paintComponent(Graphics g1) {
		Graphics2D g = ((Graphics2D) (g1));
		pendingPaintings.clear();
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		super.paintComponent(g);
		pendingPaintings.paint(g);
	}
}

