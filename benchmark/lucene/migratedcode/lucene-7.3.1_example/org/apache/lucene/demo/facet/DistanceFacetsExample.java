package org.apache.lucene.demo.facet;


import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.NumericDocValuesField;
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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.SloppyMath;

import static org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE;
import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
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

	public static final double EARTH_RADIUS_KM = 6371.0087714;

	public DistanceFacetsExample() {
	}

	public void index() throws IOException {
		IndexWriter writer = new IndexWriter(indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(CREATE));
		Document doc = new Document();
		doc.add(new DoublePoint("latitude", 40.759011));
		doc.add(new NumericDocValuesField("latitude", Double.doubleToRawLongBits(40.759011)));
		doc.add(new DoublePoint("longitude", (-73.9844722)));
		doc.add(new NumericDocValuesField("longitude", Double.doubleToRawLongBits((-73.9844722))));
		writer.addDocument(doc);
		doc = new Document();
		doc.add(new DoublePoint("latitude", 40.718266));
		doc.add(new NumericDocValuesField("latitude", Double.doubleToRawLongBits(40.718266)));
		doc.add(new DoublePoint("longitude", (-74.007819)));
		doc.add(new NumericDocValuesField("longitude", Double.doubleToRawLongBits((-74.007819))));
		writer.addDocument(doc);
		doc = new Document();
		doc.add(new DoublePoint("latitude", 40.7051157));
		doc.add(new NumericDocValuesField("latitude", Double.doubleToRawLongBits(40.7051157)));
		doc.add(new DoublePoint("longitude", (-74.0088305)));
		doc.add(new NumericDocValuesField("longitude", Double.doubleToRawLongBits((-74.0088305))));
		writer.addDocument(doc);
		searcher = new IndexSearcher(DirectoryReader.open(writer));
		writer.close();
	}

	private DoubleValuesSource getDistanceValueSource() {
		Expression distance;
		try {
			distance = JavascriptCompiler.compile((((("haversin(" + (DistanceFacetsExample.ORIGIN_LATITUDE)) + ",") + (DistanceFacetsExample.ORIGIN_LONGITUDE)) + ",latitude,longitude)"));
		} catch (ParseException pe) {
			throw new RuntimeException(pe);
		}
		SimpleBindings bindings = new SimpleBindings();
		bindings.add(new SortField("latitude", DOUBLE));
		bindings.add(new SortField("longitude", DOUBLE));
		return distance.getDoubleValuesSource(bindings);
	}

	public static Query getBoundingBoxQuery(double originLat, double originLng, double maxDistanceKM) {
		double originLatRadians = SloppyMath.toRadians(originLat);
		double originLngRadians = SloppyMath.toRadians(originLng);
		double angle = maxDistanceKM / (DistanceFacetsExample.EARTH_RADIUS_KM);
		double minLat = originLatRadians - angle;
		double maxLat = originLatRadians + angle;
		double minLng;
		double maxLng;
		if ((minLat > (SloppyMath.toRadians((-90)))) && (maxLat < (SloppyMath.toRadians(90)))) {
			double delta = Math.asin(((Math.sin(angle)) / (Math.cos(originLatRadians))));
			minLng = originLngRadians - delta;
			if (minLng < (SloppyMath.toRadians((-180)))) {
				minLng += 2 * (Math.PI);
			}
			maxLng = originLngRadians + delta;
			if (maxLng > (SloppyMath.toRadians(180))) {
				maxLng -= 2 * (Math.PI);
			}
		}else {
			minLat = Math.max(minLat, SloppyMath.toRadians((-90)));
			maxLat = Math.min(maxLat, SloppyMath.toRadians(90));
			minLng = SloppyMath.toRadians((-180));
			maxLng = SloppyMath.toRadians(180);
		}
		BooleanQuery.Builder f = new BooleanQuery.Builder();
		f.add(DoublePoint.newRangeQuery("latitude", SloppyMath.toDegrees(minLat), SloppyMath.toDegrees(maxLat)), FILTER);
		if (minLng > maxLng) {
			BooleanQuery.Builder lonF = new BooleanQuery.Builder();
			lonF.add(DoublePoint.newRangeQuery("longitude", SloppyMath.toDegrees(minLng), Double.POSITIVE_INFINITY), SHOULD);
			lonF.add(DoublePoint.newRangeQuery("longitude", Double.NEGATIVE_INFINITY, SloppyMath.toDegrees(maxLng)), SHOULD);
			f.add(lonF.build(), MUST);
		}else {
			f.add(DoublePoint.newRangeQuery("longitude", SloppyMath.toDegrees(minLng), SloppyMath.toDegrees(maxLng)), FILTER);
		}
		return f.build();
	}

	public FacetResult search() throws IOException {
		FacetsCollector fc = new FacetsCollector();
		searcher.search(new MatchAllDocsQuery(), fc);
		Facets facets = new DoubleRangeFacetCounts("field", getDistanceValueSource(), fc, DistanceFacetsExample.getBoundingBoxQuery(DistanceFacetsExample.ORIGIN_LATITUDE, DistanceFacetsExample.ORIGIN_LONGITUDE, 10.0), ONE_KM, TWO_KM, FIVE_KM, TEN_KM);
		return facets.getTopChildren(10, "field");
	}

	public TopDocs drillDown(DoubleRange range) throws IOException {
		DrillDownQuery q = new DrillDownQuery(null);
		final DoubleValuesSource vs = getDistanceValueSource();
		q.add("field", range.getQuery(DistanceFacetsExample.getBoundingBoxQuery(DistanceFacetsExample.ORIGIN_LATITUDE, DistanceFacetsExample.ORIGIN_LONGITUDE, range.max), vs));
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

	public static void main(String[] args) throws Exception {
		DistanceFacetsExample example = new DistanceFacetsExample();
		example.index();
		System.out.println("Distance facet counting example:");
		System.out.println("-----------------------");
		System.out.println(example.search());
		System.out.println("Distance facet drill-down example (field/< 2 km):");
		System.out.println("---------------------------------------------");
		TopDocs hits = example.drillDown(example.TWO_KM);
		System.out.println(((hits.totalHits) + " totalHits"));
		example.close();
	}
}

