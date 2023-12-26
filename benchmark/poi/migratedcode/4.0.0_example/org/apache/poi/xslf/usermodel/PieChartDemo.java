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
import org.apache.poi.xddf.usermodel.chart.XDDFCategoryDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFChart;
import org.apache.poi.xddf.usermodel.chart.XDDFChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;
import org.apache.poi.xddf.usermodel.chart.XDDFNumericalDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFPieChartData;


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
		try (FileInputStream argIS = new FileInputStream(args[0]);BufferedReader modelReader = new BufferedReader(new FileReader(args[1]))) {
			String chartTitle = modelReader.readLine();
			try (XMLSlideShow pptx = new XMLSlideShow(argIS)) {
				XSLFSlide slide = pptx.getSlides().get(0);
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
				List<XDDFChartData> series = chart.getChartSeries();
				XDDFPieChartData pie = ((XDDFPieChartData) (series.get(0)));
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
				final int numOfPoints = categories.length;
				final String categoryDataRange = chart.formatRange(new CellRangeAddress(1, numOfPoints, 0, 0));
				final String valuesDataRange = chart.formatRange(new CellRangeAddress(1, numOfPoints, 1, 1));
				final XDDFDataSource<?> categoriesData = XDDFDataSourcesFactory.fromArray(categories, categoryDataRange);
				final XDDFNumericalDataSource<? extends Number> valuesData = XDDFDataSourcesFactory.fromArray(values, valuesDataRange);
				XDDFPieChartData.Series firstSeries = ((XDDFPieChartData.Series) (pie.getSeries().get(0)));
				firstSeries.replaceData(categoriesData, valuesData);
				firstSeries.setExplosion(25);
				chart.plot(pie);
				try (final OutputStream out = new FileOutputStream("pie-chart-demo-output.pptx")) {
					pptx.write(out);
				}
			}
		}
	}
}

