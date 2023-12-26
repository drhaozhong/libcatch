package org.apache.poi.hssf.usermodel.examples;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.usermodel.examples.InCellLists.MultiLevelListItem;


public class InCellLists {
	private static final char BULLET_CHARACTER = '\u2022';

	private static final String TAB = "    ";

	public void demonstrateMethodCalls(String outputFilename) {
		HSSFWorkbook workbook = null;
		HSSFSheet sheet = null;
		HSSFRow row = null;
		HSSFCell cell = null;
		File outputFile = null;
		FileOutputStream fos = null;
		ArrayList<InCellLists.MultiLevelListItem> multiLevelListItems = null;
		ArrayList<String> listItems = null;
		String listItem = null;
		try {
			workbook = new HSSFWorkbook();
			sheet = workbook.createSheet("In Cell Lists");
			row = sheet.createRow(0);
			cell = row.createCell(0);
			this.bulletedItemInCell(workbook, "List Item", cell);
			row = sheet.createRow(1);
			cell = row.createCell(0);
			listItems = new ArrayList<String>();
			listItems.add("List Item One.");
			listItems.add("List Item Two.");
			listItems.add("List Item Three.");
			listItems.add("List Item Four.");
			this.listInCell(workbook, listItems, cell);
			row.setHeight(((short) (1100)));
			sheet.setColumnWidth(0, 9500);
			row = sheet.createRow(2);
			cell = row.createCell(0);
			listItems.add("List Item Five.");
			listItems.add("List Item Six.");
			this.numberedListInCell(workbook, listItems, cell, 1, 2);
			row.setHeight(((short) (1550)));
			row = sheet.createRow(3);
			cell = row.createCell(0);
			listItems.add("List Item Seven.");
			listItems.add("List Item Eight.");
			listItems.add("List Item Nine.");
			listItems.add("List Item Ten.");
			this.bulletedListInCell(workbook, listItems, cell);
			row.setHeight(((short) (2550)));
			row = sheet.createRow(4);
			cell = row.createCell(0);
			multiLevelListItems = new ArrayList<InCellLists.MultiLevelListItem>();
			listItems = new ArrayList<String>();
			listItems.add("ML List Item One - Sub Item One.");
			listItems.add("ML List Item One - Sub Item Two.");
			listItems.add("ML List Item One - Sub Item Three.");
			listItems.add("ML List Item One - Sub Item Four.");
			multiLevelListItems.add(new InCellLists.MultiLevelListItem("List Item One.", listItems));
			multiLevelListItems.add(new InCellLists.MultiLevelListItem("List Item Two.", null));
			multiLevelListItems.add(new InCellLists.MultiLevelListItem("List Item Three.", null));
			listItems = new ArrayList<String>();
			listItems.add("ML List Item Four - Sub Item One.");
			listItems.add("ML List Item Four - Sub Item Two.");
			listItems.add("ML List Item Four - Sub Item Three.");
			multiLevelListItems.add(new InCellLists.MultiLevelListItem("List Item Four.", listItems));
			this.multiLevelListInCell(workbook, multiLevelListItems, cell);
			row.setHeight(((short) (2800)));
			row = sheet.createRow(5);
			cell = row.createCell(0);
			this.multiLevelNumberedListInCell(workbook, multiLevelListItems, cell, 1, 1, 1, 2);
			row.setHeight(((short) (2800)));
			row = sheet.createRow(6);
			cell = row.createCell(0);
			this.multiLevelBulletedListInCell(workbook, multiLevelListItems, cell);
			row.setHeight(((short) (2800)));
			outputFile = new File(outputFilename);
			fos = new FileOutputStream(outputFile);
			workbook.write(fos);
		} catch (FileNotFoundException fnfEx) {
			System.out.println(("Caught a: " + (fnfEx.getClass().getName())));
			System.out.println(("Message: " + (fnfEx.getMessage())));
			System.out.println("Stacktrace follows...........");
			fnfEx.printStackTrace(System.out);
		} catch (IOException ioEx) {
			System.out.println(("Caught a: " + (ioEx.getClass().getName())));
			System.out.println(("Message: " + (ioEx.getMessage())));
			System.out.println("Stacktrace follows...........");
			ioEx.printStackTrace(System.out);
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException ioEx) {
				}
			}
		}
	}

	public void bulletedItemInCell(HSSFWorkbook workbook, String listItem, HSSFCell cell) {
		HSSFDataFormat format = workbook.createDataFormat();
		String formatString = (InCellLists.BULLET_CHARACTER) + " @";
		int formatIndex = format.getFormat(formatString);
		HSSFCellStyle bulletStyle = workbook.createCellStyle();
		bulletStyle.setDataFormat(((short) (formatIndex)));
		cell.setCellValue(new HSSFRichTextString(listItem));
		cell.setCellStyle(bulletStyle);
	}

	public void listInCell(HSSFWorkbook workbook, ArrayList<String> listItems, HSSFCell cell) {
		StringBuffer buffer = new StringBuffer();
		HSSFCellStyle wrapStyle = workbook.createCellStyle();
		wrapStyle.setWrapText(true);
		for (String listItem : listItems) {
			buffer.append(listItem);
			buffer.append("\n");
		}
		cell.setCellValue(new HSSFRichTextString(buffer.toString().trim()));
		cell.setCellStyle(wrapStyle);
	}

	public void numberedListInCell(HSSFWorkbook workbook, ArrayList<String> listItems, HSSFCell cell, int startingValue, int increment) {
		StringBuffer buffer = new StringBuffer();
		int itemNumber = startingValue;
		HSSFCellStyle wrapStyle = workbook.createCellStyle();
		wrapStyle.setWrapText(true);
		for (String listItem : listItems) {
			buffer.append(((String.valueOf(itemNumber)) + ". "));
			buffer.append(listItem);
			buffer.append("\n");
			itemNumber += increment;
		}
		cell.setCellValue(new HSSFRichTextString(buffer.toString().trim()));
		cell.setCellStyle(wrapStyle);
	}

	public void bulletedListInCell(HSSFWorkbook workbook, ArrayList<String> listItems, HSSFCell cell) {
		StringBuffer buffer = new StringBuffer();
		HSSFCellStyle wrapStyle = workbook.createCellStyle();
		wrapStyle.setWrapText(true);
		for (String listItem : listItems) {
			buffer.append(((InCellLists.BULLET_CHARACTER) + " "));
			buffer.append(listItem);
			buffer.append("\n");
		}
		cell.setCellValue(new HSSFRichTextString(buffer.toString().trim()));
		cell.setCellStyle(wrapStyle);
	}

	public void multiLevelListInCell(HSSFWorkbook workbook, ArrayList<InCellLists.MultiLevelListItem> multiLevelListItems, HSSFCell cell) {
		StringBuffer buffer = new StringBuffer();
		ArrayList<String> lowerLevelItems = null;
		HSSFCellStyle wrapStyle = workbook.createCellStyle();
		wrapStyle.setWrapText(true);
		for (InCellLists.MultiLevelListItem multiLevelListItem : multiLevelListItems) {
			buffer.append(multiLevelListItem.getItemText());
			buffer.append("\n");
			lowerLevelItems = multiLevelListItem.getLowerLevelItems();
			if ((!(lowerLevelItems == null)) && (!(lowerLevelItems.isEmpty()))) {
				for (String item : lowerLevelItems) {
					buffer.append(InCellLists.TAB);
					buffer.append(item);
					buffer.append("\n");
				}
			}
		}
		cell.setCellValue(new HSSFRichTextString(buffer.toString().trim()));
		cell.setCellStyle(wrapStyle);
	}

	public void multiLevelNumberedListInCell(HSSFWorkbook workbook, ArrayList<InCellLists.MultiLevelListItem> multiLevelListItems, HSSFCell cell, int highLevelStartingValue, int highLevelIncrement, int lowLevelStartingValue, int lowLevelIncrement) {
		StringBuffer buffer = new StringBuffer();
		int highLevelItemNumber = highLevelStartingValue;
		int lowLevelItemNumber = 0;
		ArrayList<String> lowerLevelItems = null;
		HSSFCellStyle wrapStyle = workbook.createCellStyle();
		wrapStyle.setWrapText(true);
		for (InCellLists.MultiLevelListItem multiLevelListItem : multiLevelListItems) {
			buffer.append(String.valueOf(highLevelItemNumber));
			buffer.append(". ");
			buffer.append(multiLevelListItem.getItemText());
			buffer.append("\n");
			lowerLevelItems = multiLevelListItem.getLowerLevelItems();
			if ((!(lowerLevelItems == null)) && (!(lowerLevelItems.isEmpty()))) {
				lowLevelItemNumber = lowLevelStartingValue;
				for (String item : lowerLevelItems) {
					buffer.append(InCellLists.TAB);
					buffer.append(String.valueOf(highLevelItemNumber));
					buffer.append(".");
					buffer.append(String.valueOf(lowLevelItemNumber));
					buffer.append(" ");
					buffer.append(item);
					buffer.append("\n");
					lowLevelItemNumber += lowLevelIncrement;
				}
			}
			highLevelItemNumber += highLevelIncrement;
		}
		cell.setCellValue(new HSSFRichTextString(buffer.toString().trim()));
		cell.setCellStyle(wrapStyle);
	}

	public void multiLevelBulletedListInCell(HSSFWorkbook workbook, ArrayList<InCellLists.MultiLevelListItem> multiLevelListItems, HSSFCell cell) {
		StringBuffer buffer = new StringBuffer();
		ArrayList<String> lowerLevelItems = null;
		HSSFCellStyle wrapStyle = workbook.createCellStyle();
		wrapStyle.setWrapText(true);
		for (InCellLists.MultiLevelListItem multiLevelListItem : multiLevelListItems) {
			buffer.append(InCellLists.BULLET_CHARACTER);
			buffer.append(" ");
			buffer.append(multiLevelListItem.getItemText());
			buffer.append("\n");
			lowerLevelItems = multiLevelListItem.getLowerLevelItems();
			if ((!(lowerLevelItems == null)) && (!(lowerLevelItems.isEmpty()))) {
				for (String item : lowerLevelItems) {
					buffer.append(InCellLists.TAB);
					buffer.append(InCellLists.BULLET_CHARACTER);
					buffer.append(" ");
					buffer.append(item);
					buffer.append("\n");
				}
			}
		}
		cell.setCellValue(new HSSFRichTextString(buffer.toString().trim()));
		cell.setCellStyle(wrapStyle);
	}

	public static void main(String[] args) {
		new InCellLists().demonstrateMethodCalls("C:/temp/Latest In Cell List.xls");
	}

	public final class MultiLevelListItem {
		private String itemText = null;

		private ArrayList<String> lowerLevelItems = null;

		public MultiLevelListItem(String itemText, ArrayList<String> lowerLevelItems) {
			this.itemText = itemText;
			this.lowerLevelItems = lowerLevelItems;
		}

		public String getItemText() {
			return this.itemText;
		}

		public ArrayList<String> getLowerLevelItems() {
			return this.lowerLevelItems;
		}
	}
}

