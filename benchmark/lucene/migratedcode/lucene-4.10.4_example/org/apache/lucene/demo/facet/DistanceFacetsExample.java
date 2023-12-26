package org.apache.lucene.demo.facet;


import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.DoubleRangeFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.SloppyMath;

import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;
import static org.apache.lucene.search.SortField.Type.DOUBLE;


public class DistanceFacetsExample implements Closeable {
	final DoubleRange ONE_KM = new DoubleRange("< 1 km", 0.0, true, 1.0, false);

	final DoubleRange TWO_KM = new DoubleRange("< 2 km", 0.0, true, 2.0, false);

	final DoubleRange FIVE_KM = new DoubleRange("< 5 km", 0.0, true, 5.0, false);

	final DoubleRange TEN_KM = new DoubleRange("< 10 km", 0.0, true, 10.0, false);

	private final Directory indexDir = new RAMDirectory();

	private IndexSearcher searcher;

	private final FacetsConfig config = new FacetsConfig();

	public static final double ORIGIN_LATITUDE = 40.7143528;

	public static final double ORIGIN_LONGITUDE = -74.0059731;

	public static final double EARTH_RADIUS_KM = 6371.01;

	public DistanceFacetsExample() {
	}

	public void index() throws IOException {
		IndexWriter writer = new IndexWriter(indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()));
		Document doc = new Document();
		doc.add(new DoubleField("latitude", 40.759011, NO));
		doc.add(new DoubleField("longitude", (-73.9844722), NO));
		writer.addDocument(doc);
		doc = new Document();
		doc.add(new DoubleField("latitude", 40.718266, NO));
		doc.add(new DoubleField("longitude", (-74.007819), NO));
		writer.addDocument(doc);
		doc = new Document();
		doc.add(new DoubleField("latitude", 40.7051157, NO));
		doc.add(new DoubleField("longitude", (-74.0088305), NO));
		writer.addDocument(doc);
		searcher = new IndexSearcher(DirectoryReader.open(writer, true));
		writer.close();
	}

	private ValueSource getDistanceValueSource() {
		Expression distance;
		try {
			distance = JavascriptCompiler.compile((((("haversin(" + (DistanceFacetsExample.ORIGIN_LATITUDE)) + ",") + (DistanceFacetsExample.ORIGIN_LONGITUDE)) + ",latitude,longitude)"));
		} catch (ParseException pe) {
			throw new RuntimeException(pe);
		}
		SimpleBindings bindings = new SimpleBindings();
		bindings.add(new SortField("latitude", DOUBLE));
		bindings.add(new SortField("longitude", DOUBLE));
		return distance.getValueSource(bindings);
	}

	public static Filter getBoundingBoxFilter(double originLat, double originLng, double maxDistanceKM) {
		double originLatRadians = Math.toRadians(originLat);
		double originLngRadians = Math.toRadians(originLng);
		double angle = maxDistanceKM / ((SloppyMath.earthDiameter(originLat)) / 2.0);
		double minLat = originLatRadians - angle;
		double maxLat = originLatRadians + angle;
		double minLng;
		double maxLng;
		if ((minLat > (Math.toRadians((-90)))) && (maxLat < (Math.toRadians(90)))) {
			double delta = Math.asin(((Math.sin(angle)) / (Math.cos(originLatRadians))));
			minLng = originLngRadians - delta;
			if (minLng < (Math.toRadians((-180)))) {
				minLng += 2 * (Math.PI);
			}
			maxLng = originLngRadians + delta;
			if (maxLng > (Math.toRadians(180))) {
				maxLng -= 2 * (Math.PI);
			}
		}else {
			minLat = Math.max(minLat, Math.toRadians((-90)));
			maxLat = Math.min(maxLat, Math.toRadians(90));
			minLng = Math.toRadians((-180));
			maxLng = Math.toRadians(180);
		}
		BooleanFilter f = new BooleanFilter();
		f.add(NumericRangeFilter.newDoubleRange("latitude", Math.toDegrees(minLat), Math.toDegrees(maxLat), true, true), MUST);
		if (minLng > maxLng) {
			BooleanFilter lonF = new BooleanFilter();
			lonF.add(NumericRangeFilter.newDoubleRange("longitude", Math.toDegrees(minLng), null, true, true), SHOULD);
			lonF.add(NumericRangeFilter.newDoubleRange("longitude", null, Math.toDegrees(maxLng), true, true), SHOULD);
			f.add(lonF, MUST);
		}else {
			f.add(NumericRangeFilter.newDoubleRange("longitude", Math.toDegrees(minLng), Math.toDegrees(maxLng), true, true), MUST);
		}
		return f;
	}

	public FacetResult search() throws IOException {
		FacetsCollector fc = new FacetsCollector();
		searcher.search(new MatchAllDocsQuery(), fc);
		Facets facets = new DoubleRangeFacetCounts("field", getDistanceValueSource(), fc, DistanceFacetsExample.getBoundingBoxFilter(DistanceFacetsExample.ORIGIN_LATITUDE, DistanceFacetsExample.ORIGIN_LONGITUDE, 10.0), ONE_KM, TWO_KM, FIVE_KM, TEN_KM);
		return facets.getTopChildren(10, "field");
	}

	public TopDocs drillDown(DoubleRange range) throws IOException {
		DrillDownQuery q = new DrillDownQuery(null);
		final ValueSource vs = getDistanceValueSource();
		q.add("field", range.getFilter(DistanceFacetsExample.getBoundingBoxFilter(DistanceFacetsExample.ORIGIN_LATITUDE, DistanceFacetsExample.ORIGIN_LONGITUDE, range.max), vs));
		DrillSideways ds = new DrillSideways(searcher, config, ((TaxonomyReader) (null))) {
			@Override
			protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims) throws IOException {
				assert (drillSideways.length) == 1;
				return new DoubleRangeFacetCounts("field", vs, drillSideways[0], ONE_KM, TWO_KM, FIVE_KM, TEN_KM);
			}
		};
		return ds.search(q, 10).hits;
	}

	@Override
	public void close() throws IOException {
		searcher.getIndexReader().close();
		indexDir.close();
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		DistanceFacetsExample example = new DistanceFacetsExample();
		example.index();
		System.out.println("Distance facet counting example:");
		System.out.println("-----------------------");
		System.out.println(example.search());
		System.out.println("\n");
		System.out.println("Distance facet drill-down example (field/< 2 km):");
		System.out.println("---------------------------------------------");
		TopDocs hits = example.drillDown(example.TWO_KM);
		System.out.println(((hits.totalHits) + " totalHits"));
		example.close();
	}
}

