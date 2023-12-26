package org.apache.poi.xslf.usermodel;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.POIXMLDocumentPart;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTAxDataSource;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTNumData;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTNumDataSource;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTNumRef;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTNumVal;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPieChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPieSer;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPlotArea;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTSerTx;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTStrData;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTStrRef;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTStrVal;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTUnsignedInt;


public class PieChartDemo {
	private static void usage() {
		System.out.println("Usage: PieChartDemo <pie-chart-template.pptx> <pie-chart-data.txt>");
		System.out.println("    pie-chart-template.pptx     template with a pie chart");
		System.out.println(("    pie-chart-data.txt          the model to set. First line is chart title, " + "then go pairs {axis-label value}"));
	}

	public static void main(String[] args) throws Exception {
		if ((args.length) < 2) {
			PieChartDemo.usage();
			return;
		}
		BufferedReader modelReader = new BufferedReader(new FileReader(args[1]));
		try {
			String chartTitle = modelReader.readLine();
			XMLSlideShow pptx = new XMLSlideShow(new FileInputStream(args[0]));
			XSLFSlide slide = pptx.getSlides()[0];
			XSLFChart chart = null;
			for (POIXMLDocumentPart part : slide.getRelations()) {
				if (part instanceof XSLFChart) {
					chart = ((XSLFChart) (part));
					break;
				}
			}
			if (chart == null)
				throw new IllegalStateException("chart not found in the template");

			POIXMLDocumentPart xlsPart = chart.getRelations().get(0);
			XSSFWorkbook wb = new XSSFWorkbook();
			try {
				XSSFSheet sheet = wb.createSheet();
				CTChart ctChart = chart.getCTChart();
				CTPlotArea plotArea = ctChart.getPlotArea();
				CTPieChart pieChart = plotArea.getPieChartArray(0);
				CTPieSer ser = pieChart.getSerArray(0);
				CTSerTx tx = ser.getTx();
				tx.getStrRef().getStrCache().getPtArray(0).setV(chartTitle);
				sheet.createRow(0).createCell(1).setCellValue(chartTitle);
				String titleRef = new CellReference(sheet.getSheetName(), 0, 1, true, true).formatAsString();
				tx.getStrRef().setF(titleRef);
				CTAxDataSource cat = ser.getCat();
				CTStrData strData = cat.getStrRef().getStrCache();
				CTNumDataSource val = ser.getVal();
				CTNumData numData = val.getNumRef().getNumCache();
				strData.setPtArray(null);
				numData.setPtArray(null);
				int idx = 0;
				int rownum = 1;
				String ln;
				while ((ln = modelReader.readLine()) != null) {
					String[] vals = ln.split("\\s+");
					CTNumVal numVal = numData.addNewPt();
					numVal.setIdx(idx);
					numVal.setV(vals[1]);
					CTStrVal sVal = strData.addNewPt();
					sVal.setIdx(idx);
					sVal.setV(vals[0]);
					idx++;
					XSSFRow row = sheet.createRow((rownum++));
					row.createCell(0).setCellValue(vals[0]);
					row.createCell(1).setCellValue(Double.valueOf(vals[1]));
				} 
				numData.getPtCount().setVal(idx);
				strData.getPtCount().setVal(idx);
				String numDataRange = new CellRangeAddress(1, (rownum - 1), 1, 1).formatAsString(sheet.getSheetName(), true);
				val.getNumRef().setF(numDataRange);
				String axisDataRange = new CellRangeAddress(1, (rownum - 1), 0, 0).formatAsString(sheet.getSheetName(), true);
				cat.getStrRef().setF(axisDataRange);
				OutputStream xlsOut = xlsPart.getPackagePart().getOutputStream();
				try {
					wb.write(xlsOut);
				} finally {
					xlsOut.close();
				}
				OutputStream out = new FileOutputStream("pie-chart-demo-output.pptx");
				try {
					pptx.write(out);
				} finally {
					out.close();
				}
			} finally {
				wb.close();
			}
		} finally {
			modelReader.close();
		}
	}
}

