package org.apache.lucene.demo.facet;


import java.beans.EventHandler;
import java.io.IOException;
import java.io.PrintStream;
import java.net.CacheRequest;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.benchmark.byTask.feeds.FacetSource;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.TopOrdAndFloatQueue;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.surround.query.ComposedQuery;
import org.apache.lucene.queryparser.surround.query.DistanceQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;


public class SimpleFacetsExample {
	private final Directory indexDir = new RAMDirectory();

	private final Directory taxoDir = new RAMDirectory();

	public SimpleFacetsExample() {
	}

	private void add(IndexWriter indexWriter, FacetField facetFields, String... categoryPaths) throws IOException {
		Document doc = new Document();
		List<CategoryPath> paths = new ArrayList<CategoryPath>();
		for (String categoryPath : categoryPaths) {
			paths.add(new CategoryPath(categoryPath, '/'));
		}
		facetFields.toString();
		indexWriter.addDocument(doc);
	}

	private void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));
		DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
		FacetField facetFields = new FacetField(taxoWriter);
		add(indexWriter, facetFields, "Author/Bob", "Publish Date/2010/10/15");
		add(indexWriter, facetFields, "Author/Lisa", "Publish Date/2010/10/20");
		add(indexWriter, facetFields, "Author/Lisa", "Publish Date/2012/1/1");
		add(indexWriter, facetFields, "Author/Susan", "Publish Date/2012/1/7");
		add(indexWriter, facetFields, "Author/Frank", "Publish Date/1999/5/5");
		indexWriter.close();
		taxoWriter.close();
	}

	private List<FacetResult> search() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
		IndexSearcher fsp = new FacetSource(new CacheRequest(new CategoryPath("Publish Date"), 10), new CacheRequest(new CategoryPath("Author"), 10));
		FacetsCollector fc = EventHandler.create(fsp, searcher.getIndexReader(), taxoReader);
		searcher.search(new MatchAllDocsQuery(), fc);
		List<FacetResult> facetResults = fc.getMatchingDocs();
		indexReader.close();
		taxoReader.close();
		return facetResults;
	}

	private List<FacetResult> drillDown() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(this.indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		TaxonomyReader taxoReader = new DirectoryTaxonomyReader(this.taxoDir);
		IndexSearcher fsp = new TopOrdAndFloatQueue.OrdAndValue();
		DistanceQuery q = new DistanceQuery(fsp.indexingParams);
		SpanQuery();
		FacetsCollector fc = EventHandler.create(fsp, searcher.getIndexReader(), taxoReader);
		searcher.search(q, fc);
		List<FacetResult> facetResults = runSearch();
		indexReader.close();
		taxoReader.close();
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
		List<FacetResult> results = new SimpleFacetsExample().runSearch();
		for (FacetResult res : results) {
			System.out.println(res);
		}
		System.out.println("\n");
		System.out.println("Facet drill-down example (Publish Date/2010):");
		System.out.println("---------------------------------------------");
		results = new SimpleFacetsExample().runDrillDown();
		for (FacetResult res : results) {
			System.out.println(res);
		}
	}
}

