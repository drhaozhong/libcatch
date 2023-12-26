package org.apache.lucene.demo.facet;


import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;


public class SimpleSortedSetFacetsExample {
	private final Directory indexDir = new RAMDirectory();

	private final FacetsConfig config = new FacetsConfig();

	public SimpleSortedSetFacetsExample() {
	}

	private void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()));
		Document doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Bob"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "2010"));
		indexWriter.addDocument(config.build(doc));
		doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Lisa"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "2010"));
		indexWriter.addDocument(config.build(doc));
		doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Lisa"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "2012"));
		indexWriter.addDocument(config.build(doc));
		doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Susan"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "2012"));
		indexWriter.addDocument(config.build(doc));
		doc = new Document();
		doc.add(new SortedSetDocValuesFacetField("Author", "Frank"));
		doc.add(new SortedSetDocValuesFacetField("Publish Year", "1999"));
		indexWriter.addDocument(config.build(doc));
		indexWriter.close();
	}

	private List<FacetResult> search() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		SortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(indexReader);
		FacetsCollector fc = new FacetsCollector();
		FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);
		Facets facets = new SortedSetDocValuesFacetCounts(state, fc);
		List<FacetResult> results = new ArrayList<>();
		results.add(facets.getTopChildren(10, "Author"));
		results.add(facets.getTopChildren(10, "Publish Year"));
		indexReader.close();
		return results;
	}

	private FacetResult drillDown() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		SortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(indexReader);
		DrillDownQuery q = new DrillDownQuery(config);
		q.add("Publish Year", "2010");
		FacetsCollector fc = new FacetsCollector();
		FacetsCollector.search(searcher, q, 10, fc);
		Facets facets = new SortedSetDocValuesFacetCounts(state, fc);
		FacetResult result = facets.getTopChildren(10, "Author");
		indexReader.close();
		return result;
	}

	public List<FacetResult> runSearch() throws IOException {
		index();
		return search();
	}

	public FacetResult runDrillDown() throws IOException {
		index();
		return drillDown();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Facet counting example:");
		System.out.println("-----------------------");
		SimpleSortedSetFacetsExample example = new SimpleSortedSetFacetsExample();
		List<FacetResult> results = example.runSearch();
		System.out.println(("Author: " + (results.get(0))));
		System.out.println(("Publish Year: " + (results.get(0))));
		System.out.println("\n");
		System.out.println("Facet drill-down example (Publish Year/2010):");
		System.out.println("---------------------------------------------");
		System.out.println(("Author: " + (example.runDrillDown())));
	}
}

