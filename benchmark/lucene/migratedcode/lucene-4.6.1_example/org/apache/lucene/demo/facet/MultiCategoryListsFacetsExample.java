package org.apache.lucene.demo.facet;


import java.beans.EventHandler;
import java.io.IOException;
import java.io.PrintStream;
import java.net.CacheRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.benchmark.byTask.feeds.FacetSource;
import org.apache.lucene.codecs.blockterms.TermsIndexReaderBase;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.TopOrdAndFloatQueue;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;


public class MultiCategoryListsFacetsExample {
	private final FacetExamples indexingParams;

	private final Directory indexDir = new RAMDirectory();

	private final Directory taxoDir = new RAMDirectory();

	public MultiCategoryListsFacetsExample() {
		Map<CategoryPath, CategoryListParams> categoryListParams = new HashMap<CategoryPath, javax.imageio.ImageWriteParam>();
		this.indexingParams = new TermsIndexReaderBase(categoryListParams);
	}

	private void add(IndexWriter indexWriter, FacetField facetFields, String... categoryPaths) throws IOException {
		Document doc = new Document();
		List<CategoryPath> paths = new ArrayList<CategoryPath>();
		for (String categoryPath : categoryPaths) {
			paths.add(new CategoryPath(categoryPath, '/'));
		}
		indexWriter.addDocument(doc);
	}

	private void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));
		DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
		FacetField facetFields = new FacetField(taxoWriter, indexingParams);
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
		TopOrdAndFloatQueue.OrdAndValue fsp = new FacetSource(indexingParams, new CacheRequest(new CategoryPath("Publish Date"), 10), new CacheRequest(new CategoryPath("Author"), 10));
		FacetsCollector fc = EventHandler.create(fsp, searcher.getIndexReader(), taxoReader);
		searcher.search(new MatchAllDocsQuery(), fc);
		List<FacetResult> facetResults = fc.getMatchingDocs();
		indexReader.close();
		taxoReader.close();
		return facetResults;
	}

	public List<FacetResult> runSearch() throws IOException {
		index();
		return search();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Facet counting over multiple category lists example:");
		System.out.println("-----------------------");
		List<FacetResult> results = new MultiCategoryListsFacetsExample().runSearch();
		for (FacetResult res : results) {
			System.out.println(res);
		}
	}
}

