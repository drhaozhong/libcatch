package org.apache.poi.hssf.view;


import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.io.PrintStream;
import java.util.EventObject;
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
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;

import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.BLACK;
import static org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined.WHITE;


public class SVTableCellEditor extends AbstractCellEditor implements ActionListener , TableCellEditor {
	private static final Color black = SVTableUtils.getAWTColor(BLACK);

	private static final Color white = SVTableUtils.getAWTColor(WHITE);

	private HSSFWorkbook wb;

	private JTextField editor;

	public SVTableCellEditor(HSSFWorkbook wb) {
		this.wb = wb;
		this.editor = new JTextField();
	}

	@Override
	public boolean isCellEditable(EventObject e) {
		if (e instanceof MouseEvent) {
			return (((MouseEvent) (e)).getClickCount()) >= 2;
		}
		return false;
	}

	@Override
	public boolean shouldSelectCell(EventObject anEvent) {
		return true;
	}

	public boolean startCellEditing(EventObject anEvent) {
		System.out.println("Start Cell Editing");
		return true;
	}

	@Override
	public boolean stopCellEditing() {
		System.out.println("Stop Cell Editing");
		fireEditingStopped();
		return true;
	}

	@Override
	public void cancelCellEditing() {
		System.out.println("Cancel Cell Editing");
		fireEditingCanceled();
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		System.out.println("Action performed");
		stopCellEditing();
	}

	@Override
	public Object getCellEditorValue() {
		System.out.println("GetCellEditorValue");
		return editor.getText();
	}

	@Override
	public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
		System.out.println("GetTableCellEditorComponent");
		HSSFCell cell = ((HSSFCell) (value));
		if (cell != null) {
			HSSFCellStyle style = cell.getCellStyle();
			HSSFFont f = wb.getFontAt(style.getFontIndex());
			boolean isbold = f.getBold();
			boolean isitalics = f.getItalic();
			int fontstyle = Font.PLAIN;
			if (isbold) {
				fontstyle = Font.BOLD;
			}
			if (isitalics) {
				fontstyle = fontstyle | (Font.ITALIC);
			}
			int fontheight = f.getFontHeightInPoints();
			if (fontheight == 9) {
				fontheight = 10;
			}
			Font font = new Font(f.getFontName(), fontstyle, fontheight);
			editor.setFont(font);
			if ((style.getFillPatternEnum()) == (FillPatternType.SOLID_FOREGROUND)) {
				editor.setBackground(SVTableUtils.getAWTColor(style.getFillForegroundColor(), SVTableCellEditor.white));
			}else {
				editor.setBackground(SVTableCellEditor.white);
			}
			editor.setForeground(SVTableUtils.getAWTColor(f.getColor(), SVTableCellEditor.black));
			switch (cell.getCellTypeEnum()) {
				case BLANK :
					editor.setText("");
					break;
				case BOOLEAN :
					if (cell.getBooleanCellValue()) {
						editor.setText("true");
					}else {
						editor.setText("false");
					}
					break;
				case NUMERIC :
					editor.setText(Double.toString(cell.getNumericCellValue()));
					break;
				case STRING :
					editor.setText(cell.getRichStringCellValue().getString());
					break;
				case FORMULA :
				default :
					editor.setText("?");
			}
			switch (style.getAlignmentEnum()) {
				case LEFT :
				case JUSTIFY :
				case FILL :
					editor.setHorizontalAlignment(SwingConstants.LEFT);
					break;
				case CENTER :
				case CENTER_SELECTION :
					editor.setHorizontalAlignment(SwingConstants.CENTER);
					break;
				case GENERAL :
				case RIGHT :
					editor.setHorizontalAlignment(SwingConstants.RIGHT);
					break;
				default :
					editor.setHorizontalAlignment(SwingConstants.LEFT);
					break;
			}
		}
		return editor;
	}
}

