package org.apache.lucene.demo.facet;


import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.Range;
import org.apache.lucene.facet.range.RangeAccumulator;
import org.apache.lucene.facet.range.RangeFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import static org.apache.lucene.document.Field.Store.NO;


public class RangeFacetsExample implements Closeable {
	private final Directory indexDir = new RAMDirectory();

	private IndexSearcher searcher;

	private final long nowSec = System.currentTimeMillis();

	public RangeFacetsExample() {
	}

	public void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));
		for (int i = 0; i < 100; i++) {
			Document doc = new Document();
			long then = (nowSec) - (i * 1000);
			doc.add(new NumericDocValuesField("timestamp", then));
			doc.add(new LongField("timestamp", then, NO));
			indexWriter.addDocument(doc);
		}
		searcher = new IndexSearcher(DirectoryReader.open(indexWriter, true));
		indexWriter.close();
	}

	public List<FacetResult> search() throws IOException {
		FacetSearchParams fsp = new FacetSearchParams(new RangeFacetRequest<LongRange>("timestamp", new LongRange("Past hour", ((nowSec) - 3600), true, nowSec, true), new LongRange("Past six hours", ((nowSec) - (6 * 3600)), true, nowSec, true), new LongRange("Past day", ((nowSec) - (24 * 3600)), true, nowSec, true)));
		FacetsCollector fc = FacetsCollector.create(new RangeAccumulator(fsp, searcher.getIndexReader()));
		searcher.search(new MatchAllDocsQuery(), fc);
		return fc.getFacetResults();
	}

	public TopDocs drillDown(LongRange range) throws IOException {
		DrillDownQuery q = new DrillDownQuery(FacetIndexingParams.DEFAULT);
		q.add("timestamp", NumericRangeQuery.newLongRange("timestamp", range.min, range.max, range.minInclusive, range.maxInclusive));
		return searcher.search(q, 10);
	}

	public void close() throws IOException {
		searcher.getIndexReader().close();
		indexDir.close();
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		RangeFacetsExample example = new RangeFacetsExample();
		example.index();
		System.out.println("Facet counting example:");
		System.out.println("-----------------------");
		List<FacetResult> results = example.search();
		for (FacetResult res : results) {
			System.out.println(res);
		}
		System.out.println("\n");
		System.out.println("Facet drill-down example (timestamp/Past six hours):");
		System.out.println("---------------------------------------------");
		TopDocs hits = example.drillDown(((LongRange) (((RangeFacetRequest<LongRange>) (results.get(0).getFacetRequest())).ranges[1])));
		System.out.println(((hits.totalHits) + " totalHits"));
		example.close();
	}
}

