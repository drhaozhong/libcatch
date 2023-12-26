package org.apache.poi.hssf.view;


import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.Rectangle;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import javax.swing.table.TableCellRenderer;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;


public class SVTableCellRenderer extends JLabel implements Serializable , TableCellRenderer {
	protected static Border noFocusBorder = new EmptyBorder(1, 1, 1, 1);

	protected SVBorder cellBorder = new SVBorder();

	private HSSFWorkbook wb = null;

	private class CellFormatter {
		private Format[] textFormatter;

		private DecimalFormat generalNumberFormat = new DecimalFormat("0");

		public CellFormatter() {
			textFormatter = new Format[49];
			textFormatter[1] = new DecimalFormat("0");
			textFormatter[2] = new DecimalFormat("0.00");
			textFormatter[3] = new DecimalFormat("#,##0");
			textFormatter[4] = new DecimalFormat("#,##0.00");
			textFormatter[5] = new DecimalFormat("$#,##0;$#,##0");
			textFormatter[6] = new DecimalFormat("$#,##0;$#,##0");
			textFormatter[7] = new DecimalFormat("$#,##0.00;$#,##0.00");
			textFormatter[8] = new DecimalFormat("$#,##0.00;$#,##0.00");
			textFormatter[9] = new DecimalFormat("0%");
			textFormatter[10] = new DecimalFormat("0.00%");
			textFormatter[11] = new DecimalFormat("0.00E0");
			textFormatter[12] = new SVFractionalFormat("# ?/?");
			textFormatter[13] = new SVFractionalFormat("# ??/??");
			textFormatter[14] = new SimpleDateFormat("M/d/yy");
			textFormatter[15] = new SimpleDateFormat("d-MMM-yy");
			textFormatter[16] = new SimpleDateFormat("d-MMM");
			textFormatter[17] = new SimpleDateFormat("MMM-yy");
			textFormatter[18] = new SimpleDateFormat("h:mm a");
			textFormatter[19] = new SimpleDateFormat("h:mm:ss a");
			textFormatter[20] = new SimpleDateFormat("h:mm");
			textFormatter[21] = new SimpleDateFormat("h:mm:ss");
			textFormatter[22] = new SimpleDateFormat("M/d/yy h:mm");
			textFormatter[38] = new DecimalFormat("#,##0;#,##0");
			textFormatter[39] = new DecimalFormat("#,##0.00;#,##0.00");
			textFormatter[40] = new DecimalFormat("#,##0.00;#,##0.00");
			textFormatter[45] = new SimpleDateFormat("mm:ss");
			textFormatter[47] = new SimpleDateFormat("mm:ss.0");
			textFormatter[48] = new DecimalFormat("##0.0E0");
		}

		public String format(short index, double value) {
			if (index <= 0)
				return generalNumberFormat.format(value);

			if ((textFormatter[index]) == null)
				throw new RuntimeException(("Sorry. I cant handle the format code :" + (Integer.toHexString(index))));

			if ((textFormatter[index]) instanceof DecimalFormat) {
				return ((DecimalFormat) (textFormatter[index])).format(value);
			}
			if ((textFormatter[index]) instanceof SVFractionalFormat) {
				return ((SVFractionalFormat) (textFormatter[index])).format(value);
			}
			throw new RuntimeException(("Sorry. I cant handle a non decimal formatter for a decimal value :" + (Integer.toHexString(index))));
		}

		public boolean useRedColor(short index, double value) {
			return ((((index == 6) || (index == 8)) || (index == 38)) || (index == 39)) && (value < 0);
		}
	}

	private final SVTableCellRenderer.CellFormatter cellFormatter = new SVTableCellRenderer.CellFormatter();

	public SVTableCellRenderer(HSSFWorkbook wb) {
		super();
		setOpaque(true);
		setBorder(SVTableCellRenderer.noFocusBorder);
		this.wb = wb;
	}

	public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
		boolean isBorderSet = false;
		HSSFCell c = ((HSSFCell) (value));
		if (c != null) {
			HSSFCellStyle s = c.getCellStyle();
			HSSFFont f = wb.getFontAt(s.getFontIndex());
			setFont(SVTableUtils.makeFont(f));
			if ((s.getFillPatternEnum()) == (FillPatternType.SOLID_FOREGROUND)) {
				setBackground(SVTableUtils.getAWTColor(s.getFillForegroundColor(), SVTableUtils.white));
			}else
				setBackground(SVTableUtils.white);

			setForeground(SVTableUtils.getAWTColor(f.getColor(), SVTableUtils.black));
			cellBorder.setBorder(SVTableUtils.getAWTColor(s.getTopBorderColor(), SVTableUtils.black), SVTableUtils.getAWTColor(s.getRightBorderColor(), SVTableUtils.black), SVTableUtils.getAWTColor(s.getBottomBorderColor(), SVTableUtils.black), SVTableUtils.getAWTColor(s.getLeftBorderColor(), SVTableUtils.black), s.getBorderTopEnum(), s.getBorderRightEnum(), s.getBorderBottomEnum(), s.getBorderLeftEnum(), hasFocus);
			setBorder(cellBorder);
			isBorderSet = true;
			switch (c.getCellTypeEnum()) {
				case BLANK :
					setValue("");
					break;
				case BOOLEAN :
					if (c.getBooleanCellValue()) {
						setValue("true");
					}else {
						setValue("false");
					}
					break;
				case NUMERIC :
					short format = s.getDataFormat();
					double numericValue = c.getNumericCellValue();
					if (cellFormatter.useRedColor(format, numericValue))
						setForeground(Color.red);
					else
						setForeground(null);

					setValue(cellFormatter.format(format, c.getNumericCellValue()));
					break;
				case STRING :
					setValue(c.getRichStringCellValue().getString());
					break;
				case FORMULA :
				default :
					setValue("?");
			}
			switch (s.getAlignmentEnum()) {
				case LEFT :
				case JUSTIFY :
				case FILL :
					setHorizontalAlignment(SwingConstants.LEFT);
					break;
				case CENTER :
				case CENTER_SELECTION :
					setHorizontalAlignment(SwingConstants.CENTER);
					break;
				case GENERAL :
				case RIGHT :
					setHorizontalAlignment(SwingConstants.RIGHT);
					break;
				default :
					setHorizontalAlignment(SwingConstants.LEFT);
					break;
			}
		}else {
			setValue("");
			setBackground(SVTableUtils.white);
		}
		if (hasFocus) {
			if (!isBorderSet) {
				cellBorder.setBorder(SVTableUtils.black, SVTableUtils.black, SVTableUtils.black, SVTableUtils.black, BorderStyle.NONE, BorderStyle.NONE, BorderStyle.NONE, BorderStyle.NONE, isSelected);
				setBorder(cellBorder);
			}
			if (table.isCellEditable(row, column)) {
				setForeground(UIManager.getColor("Table.focusCellForeground"));
				setBackground(UIManager.getColor("Table.focusCellBackground"));
			}
		}else
			if (!isBorderSet) {
				setBorder(SVTableCellRenderer.noFocusBorder);
			}

		Color back = getBackground();
		boolean colorMatch = ((back != null) && (back.equals(table.getBackground()))) && (table.isOpaque());
		setOpaque((!colorMatch));
		return this;
	}

	public void validate() {
	}

	public void revalidate() {
	}

	public void repaint(long tm, int x, int y, int width, int height) {
	}

	public void repaint(Rectangle r) {
	}

	protected void firePropertyChange(String propertyName, Object oldValue, Object newValue) {
		if (propertyName == "text") {
			super.firePropertyChange(propertyName, oldValue, newValue);
		}
	}

	public void firePropertyChange(String propertyName, boolean oldValue, boolean newValue) {
	}

	protected void setValue(Object value) {
		setText((value == null ? "" : value.toString()));
	}
}

