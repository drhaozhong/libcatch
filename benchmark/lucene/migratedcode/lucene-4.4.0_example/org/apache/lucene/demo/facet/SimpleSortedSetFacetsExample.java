package org.apache.lucene.demo.facet;


import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesAccumulator;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetFields;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;


public class SimpleSortedSetFacetsExample {
	private final Directory indexDir = new RAMDirectory();

	public SimpleSortedSetFacetsExample() {
	}

	private void add(IndexWriter indexWriter, SortedSetDocValuesFacetFields facetFields, String... categoryPaths) throws IOException {
		Document doc = new Document();
		List<CategoryPath> paths = new ArrayList<CategoryPath>();
		for (String categoryPath : categoryPaths) {
			paths.add(new CategoryPath(categoryPath, '/'));
		}
		facetFields.addFields(doc, paths);
		indexWriter.addDocument(doc);
	}

	private void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));
		SortedSetDocValuesFacetFields facetFields = new SortedSetDocValuesFacetFields();
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
		FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("Publish Year"), 10), new CountFacetRequest(new CategoryPath("Author"), 10));
		FacetsCollector fc = FacetsCollector.create(new SortedSetDocValuesAccumulator(state, fsp));
		searcher.search(new MatchAllDocsQuery(), fc);
		List<FacetResult> facetResults = fc.getFacetResults();
		indexReader.close();
		return facetResults;
	}

	private List<FacetResult> drillDown() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(indexReader);
		FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("Author"), 10));
		DrillDownQuery q = new DrillDownQuery(fsp.indexingParams, new MatchAllDocsQuery());
		q.add(new CategoryPath("Publish Year/2010", '/'));
		FacetsCollector fc = FacetsCollector.create(new SortedSetDocValuesAccumulator(state, fsp));
		searcher.search(q, fc);
		List<FacetResult> facetResults = fc.getFacetResults();
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

