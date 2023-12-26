package org.apache.lucene.demo.facet;


import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetSumValueSource;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.facet.taxonomy.TaxonomyFacetSumValueSource.ScoreValueSource.<init>;
import static org.apache.lucene.search.SortField.Type.LONG;
import static org.apache.lucene.search.SortField.Type.SCORE;


public class ExpressionAggregationFacetsExample {
	private final Directory indexDir = new RAMDirectory();

	private final Directory taxoDir = new RAMDirectory();

	public ExpressionAggregationFacetsExample() {
	}

	private void add(IndexWriter indexWriter, FacetField facetFields, String text, String category, long popularity) throws IOException {
		Document doc = new Document();
		doc.add(new TextField("c", text, NO));
		doc.add(new NumericDocValuesField("popularity", popularity));
		indexWriter.addDocument(doc);
	}

	private void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));
		DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
		FacetField facetFields = new FacetField(taxoWriter);
		add(indexWriter, facetFields, "foo bar", "A/B", 5L);
		add(indexWriter, facetFields, "foo foo bar", "A/C", 3L);
		indexWriter.close();
		taxoWriter.close();
	}

	private List<FacetResult> search() throws IOException, ParseException {
		DirectoryReader indexReader = DirectoryReader.open(indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
		Expression expr = JavascriptCompiler.compile("_score * sqrt(popularity)");
		SimpleBindings bindings = new SimpleBindings();
		bindings.add(new SortField("_score", SCORE));
		bindings.add(new SortField("popularity", LONG));
		IndexSearcher fsp = new IndexSearcher(new TaxonomyFacetSumValueSource.ScoreValueSource(new CategoryPath("A"), 10, expr.getValueSource(bindings), true));
		FacetsCollector fc = TopScoreDocCollector.create(fsp, searcher.getIndexReader(), taxoReader);
		searcher.search(new MatchAllDocsQuery(), fc);
		List<FacetResult> facetResults = fc.getMatchingDocs();
		indexReader.close();
		taxoReader.close();
		return facetResults;
	}

	public List<FacetResult> runSearch() throws IOException, ParseException {
		index();
		return search();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Facet counting example:");
		System.out.println("-----------------------");
		List<FacetResult> results = new ExpressionAggregationFacetsExample().runSearch();
		for (FacetResult res : results) {
			System.out.println(res);
		}
	}
}

