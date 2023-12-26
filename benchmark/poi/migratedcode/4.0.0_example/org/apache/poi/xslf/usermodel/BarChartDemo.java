package org.apache.poi.xslf.usermodel;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.ooxml.POIXMLDocumentPart;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xddf.usermodel.chart.AxisOrientation;
import org.apache.poi.xddf.usermodel.chart.AxisPosition;
import org.apache.poi.xddf.usermodel.chart.BarDirection;
import org.apache.poi.xddf.usermodel.chart.XDDFBarChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFCategoryAxis;
import org.apache.poi.xddf.usermodel.chart.XDDFCategoryDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFChart;
import org.apache.poi.xddf.usermodel.chart.XDDFChartAxis;
import org.apache.poi.xddf.usermodel.chart.XDDFChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;
import org.apache.poi.xddf.usermodel.chart.XDDFNumericalDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFValueAxis;


public class BarChartDemo {
	private static void usage() {
		System.out.println("Usage: BarChartDemo <bar-chart-template.pptx> <bar-chart-data.txt>");
		System.out.println("    bar-chart-template.pptx     template with a bar chart");
		System.out.println(("    bar-chart-data.txt          the model to set. First line is chart title, " + "then go pairs {axis-label value}"));
	}

	public static void main(String[] args) throws Exception {
		if ((args.length) < 2) {
			BarChartDemo.usage();
			return;
		}
		try (FileInputStream argIS = new FileInputStream(args[0]);BufferedReader modelReader = new BufferedReader(new FileReader(args[1]))) {
			String chartTitle = modelReader.readLine();
			List<String> listCategories = new ArrayList<String>(3);
			List<Double> listValues = new ArrayList<Double>(3);
			String ln;
			while ((ln = modelReader.readLine()) != null) {
				String[] vals = ln.split("\\s+");
				listCategories.add(vals[0]);
				listValues.add(Double.valueOf(vals[1]));
			} 
			String[] categories = listCategories.toArray(new String[listCategories.size()]);
			Double[] values = listValues.toArray(new Double[listValues.size()]);
			try (XMLSlideShow pptx = new XMLSlideShow(argIS)) {
				XSLFSlide slide = pptx.getSlides().get(0);
				BarChartDemo.setBarData(BarChartDemo.findChart(slide), chartTitle, categories, values);
				XSLFChart chart = BarChartDemo.findChart(pptx.createSlide().importContent(slide));
				BarChartDemo.setColumnData(chart, "Column variant");
				try (OutputStream out = new FileOutputStream("bar-chart-demo-output.pptx")) {
					pptx.write(out);
				}
			}
		}
	}

	private static void setBarData(XSLFChart chart, String chartTitle, String[] categories, Double[] values) {
		final List<XDDFChartData> series = chart.getChartSeries();
		final XDDFBarChartData bar = ((XDDFBarChartData) (series.get(0)));
		final int numOfPoints = categories.length;
		final String categoryDataRange = chart.formatRange(new CellRangeAddress(1, numOfPoints, 0, 0));
		final String valuesDataRange = chart.formatRange(new CellRangeAddress(1, numOfPoints, 1, 1));
		final XDDFDataSource<?> categoriesData = XDDFDataSourcesFactory.fromArray(categories, categoryDataRange);
		final XDDFNumericalDataSource<? extends Number> valuesData = XDDFDataSourcesFactory.fromArray(values, valuesDataRange);
		bar.getSeries().get(0).replaceData(categoriesData, valuesData);
		chart.plot(bar);
	}

	private static void setColumnData(XSLFChart chart, String chartTitle) {
		List<XDDFChartData> series = chart.getChartSeries();
		XDDFBarChartData bar = ((XDDFBarChartData) (series.get(0)));
		bar.setBarDirection(BarDirection.COL);
		bar.getCategoryAxis().setOrientation(AxisOrientation.MAX_MIN);
		bar.getValueAxes().get(0).setPosition(AxisPosition.TOP);
	}

	private static XSLFChart findChart(XSLFSlide slide) {
		XSLFChart chart = null;
		for (POIXMLDocumentPart part : slide.getRelations()) {
			if (part instanceof XSLFChart) {
				chart = ((XSLFChart) (part));
				break;
			}
		}
		if (chart == null) {
			throw new IllegalStateException("chart not found in the template");
		}
		return chart;
	}
}

