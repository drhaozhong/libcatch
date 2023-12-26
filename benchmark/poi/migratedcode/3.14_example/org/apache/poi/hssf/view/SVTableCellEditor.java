package org.apache.poi.hssf.view;


import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.io.PrintStream;
import java.util.EventObject;
import java.util.Map;
import javax.swing.AbstractCellEditor;
import javax.swing.JComponent;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.table.TableCellEditor;
import javax.swing.text.JTextComponent;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;


public class SVTableCellEditor extends AbstractCellEditor implements ActionListener , TableCellEditor {
	private static final Color black = SVTableCellEditor.getAWTColor(new HSSFColor.BLACK());

	private static final Color white = SVTableCellEditor.getAWTColor(new HSSFColor.WHITE());

	private Map<Integer, HSSFColor> colors = HSSFColor.getIndexHash();

	private HSSFWorkbook wb;

	private JTextField editor;

	public SVTableCellEditor(HSSFWorkbook wb) {
		this.wb = wb;
		this.editor = new JTextField();
	}

	public boolean isCellEditable(EventObject e) {
		if (e instanceof MouseEvent) {
			return (((MouseEvent) (e)).getClickCount()) >= 2;
		}
		return false;
	}

	public boolean shouldSelectCell(EventObject anEvent) {
		return true;
	}

	public boolean startCellEditing(EventObject anEvent) {
		System.out.println("Start Cell Editing");
		return true;
	}

	public boolean stopCellEditing() {
		System.out.println("Stop Cell Editing");
		fireEditingStopped();
		return true;
	}

	public void cancelCellEditing() {
		System.out.println("Cancel Cell Editing");
		fireEditingCanceled();
	}

	public void actionPerformed(ActionEvent e) {
		System.out.println("Action performed");
		stopCellEditing();
	}

	public Object getCellEditorValue() {
		System.out.println("GetCellEditorValue");
		return editor.getText();
	}

	public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
		System.out.println("GetTableCellEditorComponent");
		HSSFCell cell = ((HSSFCell) (value));
		if (cell != null) {
			HSSFCellStyle style = cell.getCellStyle();
			HSSFFont f = wb.getFontAt(style.getFontIndex());
			boolean isbold = (f.getBoldweight()) > (HSSFFont.BOLDWEIGHT_NORMAL);
			boolean isitalics = f.getItalic();
			int fontstyle = Font.PLAIN;
			if (isbold)
				fontstyle = Font.BOLD;

			if (isitalics)
				fontstyle = fontstyle | (Font.ITALIC);

			int fontheight = f.getFontHeightInPoints();
			if (fontheight == 9)
				fontheight = 10;

			Font font = new Font(f.getFontName(), fontstyle, fontheight);
			editor.setFont(font);
			if ((style.getFillPattern()) == (HSSFCellStyle.SOLID_FOREGROUND)) {
				editor.setBackground(getAWTColor(style.getFillForegroundColor(), SVTableCellEditor.white));
			}else
				editor.setBackground(SVTableCellEditor.white);

			editor.setForeground(getAWTColor(f.getColor(), SVTableCellEditor.black));
			switch (cell.getCellType()) {
				case HSSFCell.CELL_TYPE_BLANK :
					editor.setText("");
					break;
				case HSSFCell.CELL_TYPE_BOOLEAN :
					if (cell.getBooleanCellValue()) {
						editor.setText("true");
					}else {
						editor.setText("false");
					}
					break;
				case HSSFCell.CELL_TYPE_NUMERIC :
					editor.setText(Double.toString(cell.getNumericCellValue()));
					break;
				case HSSFCell.CELL_TYPE_STRING :
					editor.setText(cell.getRichStringCellValue().getString());
					break;
				case HSSFCell.CELL_TYPE_FORMULA :
				default :
					editor.setText("?");
			}
			switch (style.getAlignment()) {
				case HSSFCellStyle.ALIGN_LEFT :
				case HSSFCellStyle.ALIGN_JUSTIFY :
				case HSSFCellStyle.ALIGN_FILL :
					editor.setHorizontalAlignment(SwingConstants.LEFT);
					break;
				case HSSFCellStyle.ALIGN_CENTER :
				case HSSFCellStyle.ALIGN_CENTER_SELECTION :
					editor.setHorizontalAlignment(SwingConstants.CENTER);
					break;
				case HSSFCellStyle.ALIGN_GENERAL :
				case HSSFCellStyle.ALIGN_RIGHT :
					editor.setHorizontalAlignment(SwingConstants.RIGHT);
					break;
				default :
					editor.setHorizontalAlignment(SwingConstants.LEFT);
					break;
			}
		}
		return editor;
	}

	private final Color getAWTColor(int index, Color deflt) {
		HSSFColor clr = colors.get(index);
		if (clr == null)
			return deflt;

		return SVTableCellEditor.getAWTColor(clr);
	}

	private static final Color getAWTColor(HSSFColor clr) {
		short[] rgb = clr.getTriplet();
		return new Color(rgb[0], rgb[1], rgb[2]);
	}
}

