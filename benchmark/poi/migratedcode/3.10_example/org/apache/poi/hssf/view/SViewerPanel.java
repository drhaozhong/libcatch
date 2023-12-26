package org.apache.poi.hssf.view;


import java.awt.AWTEvent;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowEvent;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.plaf.TabbedPaneUI;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;


public class SViewerPanel extends JPanel {
	private static final int magicCharFactor = 7;

	HSSFWorkbook wb;

	JTabbedPane sheetPane;

	private SVTableCellRenderer cellRenderer;

	private SVTableCellEditor cellEditor;

	private boolean allowEdits;

	public SViewerPanel(HSSFWorkbook wb, boolean allowEdits) {
		this.wb = wb;
		this.allowEdits = allowEdits;
		initialiseGui();
	}

	private void initialiseGui() {
		cellRenderer = new SVTableCellRenderer(this.wb);
		if (allowEdits)
			cellEditor = new SVTableCellEditor(this.wb);

		sheetPane = new JTabbedPane(JTabbedPane.BOTTOM);
		if (allowEdits)
			sheetPane.addMouseListener(createTabListener());

		int sheetCount = wb.getNumberOfSheets();
		for (int i = 0; i < sheetCount; i++) {
			String sheetName = wb.getSheetName(i);
			sheetPane.addTab(sheetName, makeSheetView(wb.getSheetAt(i)));
		}
		setLayout(new BorderLayout());
		add(sheetPane, BorderLayout.CENTER);
	}

	protected JComponent makeSheetView(HSSFSheet sheet) {
		JTable sheetView = new JTable(new SVTableModel(sheet));
		sheetView.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		sheetView.setDefaultRenderer(HSSFCell.class, cellRenderer);
		if (allowEdits)
			sheetView.setDefaultEditor(HSSFCell.class, cellEditor);

		JTableHeader header = sheetView.getTableHeader();
		header.setReorderingAllowed(false);
		header.setResizingAllowed(allowEdits);
		TableColumnModel columns = sheetView.getColumnModel();
		for (int i = 0; i < (columns.getColumnCount()); i++) {
			TableColumn column = columns.getColumn(i);
			int width = sheet.getColumnWidth(i);
			column.setPreferredWidth(((width / 256) * (SViewerPanel.magicCharFactor)));
		}
		int rows = sheet.getPhysicalNumberOfRows();
		Insets insets = cellRenderer.getInsets();
		int extraHeight = (insets.bottom) + (insets.top);
		for (int i = 0; i < rows; i++) {
			HSSFRow row = sheet.getRow(i);
			if (row == null) {
				sheetView.setRowHeight(i, (((int) (sheet.getDefaultRowHeightInPoints())) + extraHeight));
			}else {
				sheetView.setRowHeight(i, (((int) (row.getHeightInPoints())) + extraHeight));
			}
		}
		SVRowHeader rowHeader = new SVRowHeader(sheet, sheetView, extraHeight);
		JScrollPane scroll = new JScrollPane(sheetView);
		scroll.setRowHeaderView(rowHeader);
		return scroll;
	}

	public void paint(Graphics g) {
		long start = System.currentTimeMillis();
		super.paint(g);
		long elapsed = (System.currentTimeMillis()) - start;
		System.out.println(("Paint time = " + elapsed));
	}

	protected MouseListener createTabListener() {
		return new SViewerPanel.TabListener();
	}

	private class TabListener implements MouseListener {
		public JPopupMenu popup;

		public TabListener() {
			popup = new JPopupMenu("Sheet");
			popup.add(createInsertSheetAction());
			popup.add(createDeleteSheetAction());
			popup.add(createRenameSheetAction());
		}

		protected Action createInsertSheetAction() {
			return new SViewerPanel.InsertSheetAction();
		}

		protected Action createDeleteSheetAction() {
			return new SViewerPanel.DeleteSheetAction();
		}

		protected Action createRenameSheetAction() {
			return new SViewerPanel.RenameSheetAction();
		}

		protected void checkPopup(MouseEvent e) {
			if (e.isPopupTrigger()) {
				int tab = sheetPane.getUI().tabForCoordinate(sheetPane, e.getX(), e.getY());
				if (tab != (-1)) {
					popup.show(sheetPane, e.getX(), e.getY());
				}
			}
		}

		public void mouseClicked(MouseEvent e) {
			checkPopup(e);
		}

		public void mousePressed(MouseEvent e) {
			checkPopup(e);
		}

		public void mouseReleased(MouseEvent e) {
			checkPopup(e);
		}

		public void mouseEntered(MouseEvent e) {
		}

		public void mouseExited(MouseEvent e) {
		}
	}

	private class RenameSheetAction extends AbstractAction {
		public RenameSheetAction() {
			super("Rename");
		}

		public void actionPerformed(ActionEvent e) {
			int tabIndex = sheetPane.getSelectedIndex();
			if (tabIndex != (-1)) {
				String newSheetName = JOptionPane.showInputDialog(sheetPane, "Enter a new Sheetname", "Rename Sheet", JOptionPane.QUESTION_MESSAGE);
				if (newSheetName != null) {
					wb.setSheetName(tabIndex, newSheetName);
					sheetPane.setTitleAt(tabIndex, newSheetName);
				}
			}
		}
	}

	private class InsertSheetAction extends AbstractAction {
		public InsertSheetAction() {
			super("Insert");
		}

		public void actionPerformed(ActionEvent e) {
			HSSFSheet newSheet = wb.createSheet();
			for (int i = 0; i < (wb.getNumberOfSheets()); i++) {
				HSSFSheet sheet = wb.getSheetAt(i);
				if (newSheet == sheet) {
					sheetPane.insertTab(wb.getSheetName(i), null, makeSheetView(sheet), null, i);
				}
			}
		}
	}

	private class DeleteSheetAction extends AbstractAction {
		public DeleteSheetAction() {
			super("Delete");
		}

		public void actionPerformed(ActionEvent e) {
			int tabIndex = sheetPane.getSelectedIndex();
			if (tabIndex != (-1)) {
				if ((JOptionPane.showConfirmDialog(sheetPane, "Are you sure that you want to delete the selected sheet", "Delete Sheet?", JOptionPane.OK_CANCEL_OPTION)) == (JOptionPane.OK_OPTION)) {
					wb.removeSheetAt(tabIndex);
					sheetPane.remove(tabIndex);
				}
			}
		}
	}

	public boolean isEditable() {
		return allowEdits;
	}

	public static void main(String[] args) {
		if ((args.length) < 1) {
			throw new IllegalArgumentException("A filename to view must be supplied as the first argument, but none was given");
		}
		try {
			FileInputStream in = new FileInputStream(args[0]);
			HSSFWorkbook wb = new HSSFWorkbook(in);
			in.close();
			SViewerPanel p = new SViewerPanel(wb, true);
			JFrame frame;
			frame = new JFrame() {
				protected void processWindowEvent(WindowEvent e) {
					super.processWindowEvent(e);
					if ((e.getID()) == (WindowEvent.WINDOW_CLOSING)) {
						System.exit(0);
					}
				}

				public synchronized void setTitle(String title) {
					super.setTitle(title);
					enableEvents(AWTEvent.WINDOW_EVENT_MASK);
				}
			};
			frame.setTitle("Viewer Frame");
			frame.getContentPane().add(p, BorderLayout.CENTER);
			frame.setSize(800, 640);
			Dimension d = Toolkit.getDefaultToolkit().getScreenSize();
			frame.setLocation((((d.width) - (frame.getSize().width)) / 2), (((d.height) - (frame.getSize().height)) / 2));
			frame.setVisible(true);
		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(1);
		}
	}
}

