package org.apache.lucene.demo.facet;


import java.io.IOException;
import java.io.PrintStream;
import java.net.CacheRequest;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.benchmark.byTask.feeds.FacetSource;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.TopOrdAndFloatQueue;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.org.apache.lucene.facet.sortedset.SortedSetDocValuesAccumulator;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.surround.query.ComposedQuery;
import org.apache.lucene.search.FieldCache.ByteParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.grouping.AbstractGroupFacetCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import static org.apache.lucene.facet.TopOrdAndFloatQueue.OrdAndValue.<init>;


public class SimpleSortedSetFacetsExample {
	private final Directory indexDir = new RAMDirectory();

	public SimpleSortedSetFacetsExample() {
	}

	private void add(IndexWriter indexWriter, SortedSetDocValuesFacetCounts facetFields, String... categoryPaths) throws IOException {
		Document doc = new Document();
		List<CategoryPath> paths = new ArrayList<CategoryPath>();
		for (String categoryPath : categoryPaths) {
			paths.add(new CategoryPath(categoryPath, '/'));
		}
		indexWriter.addDocument(doc);
	}

	private void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));
		SortedSetDocValuesFacetCounts facetFields = new SortedSetDocValuesFacetField();
		add(indexWriter, facetFields, "Author/Bob", "Publish Year/2010");
		add(indexWriter, facetFields, "Author/Lisa", "Publish Year/2010");
		add(indexWriter, facetFields, "Author/Lisa", "Publish Year/2012");
		add(indexWriter, facetFields, "Author/Susan", "Publish Year/2012");
		add(indexWriter, facetFields, "Author/Frank", "Publish Year/1999");
		indexWriter.close();
	}

	private List<FacetResult> search() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(indexReader);
		IndexSearcher fsp = new FacetSource(new CacheRequest(new CategoryPath("Publish Year"), 10), new CacheRequest(new CategoryPath("Author"), 10));
		FacetsCollector fc = org.apache.lucene.facet.search.FacetsCollector.create(new AbstractGroupFacetCollector(state, fsp));
		searcher.search(new MatchAllDocsQuery(), fc);
		List<FacetResult> facetResults = runSearch();
		indexReader.close();
		return facetResults;
	}

	private List<FacetResult> drillDown() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(this.indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(indexReader);
		IndexSearcher fsp = new TopOrdAndFloatQueue.OrdAndValue(new CacheRequest(new CategoryPath("Author"), 10));
		TopOrdAndFloatQueue q = new org.apache.lucene.queryparser.surround.query.DistanceQuery(fsp.indexingParams, new MatchAllDocsQuery());
		FacetsCollector fc = create(new SortedSetDocValuesAccumulator(state, fsp));
		List<FacetResult> facetResults = runSearch();
		indexReader.close();
		return facetResults;
	}

	public List<FacetResult> runSearch() throws IOException {
		index();
		return search();
	}

	public List<FacetResult> runDrillDown() throws IOException {
		index();
		return drillDown();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Facet counting example:");
		System.out.println("-----------------------");
		List<FacetResult> results = new SimpleSortedSetFacetsExample().runSearch();
		for (FacetResult res : results) {
			System.out.println(res);
		}
		System.out.println("\n");
		System.out.println("Facet drill-down example (Publish Year/2010):");
		System.out.println("---------------------------------------------");
		results = new SimpleSortedSetFacetsExample().runDrillDown();
		for (FacetResult res : results) {
			System.out.println(res);
		}
	}
}

